from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
import json
from uuid import UUID

import pytest

from advanced_options_reference_support import (
    EXPIRATION,
    KNOWN,
    SECURITY,
    SESSION,
    adjusted_bars,
    equity_metrics,
    open_interest,
    reference_state,
    valuation,
    volatility,
)
from options.model_inputs import (
    ExpirationValuationCandidate,
    RiskFreeRateObservation,
)
from realtime.advanced_options_reference_state import (
    AdvancedOptionsEquityMetric,
    AdvancedOptionsHistoricalVolatilityFact,
    InMemoryAdvancedOptionsReferenceStateRepository,
    ReferenceAvailability,
    dividend_coverage_reference_from_window,
    open_interest_reference_from_fact,
    valuation_reference_from_candidate,
)
from realtime.advanced_options_reference_repository import (
    decode_advanced_options_reference_state,
)
from realtime.contracts import canonical_json
from realtime.dividend_reference import DividendReferenceWindow
from realtime.reference_facts import ReferenceFact, ReferenceFactRelation


def test_reference_state_is_canonical_and_binds_exact_members():
    state = reference_state()
    repeated = reference_state()

    assert state == repeated
    assert len(state.member_identities) == 8
    assert len(set(state.member_identities)) == 8
    assert {
        row.window_sessions for row in state.historical_volatility
    } == {20, 30}
    assert state.open_interest[1].availability is (
        ReferenceAvailability.UNAVAILABLE
    )
    assert state.open_interest[1].open_interest is None
    assert state.open_interest_sha256


def test_persistence_document_round_trip_preserves_exact_state():
    state = reference_state()
    document = json.loads(canonical_json(state.identity))

    assert decode_advanced_options_reference_state(
        state_id=state.state_id,
        state_sha256=state.state_sha256,
        document=document,
    ) == state


def test_missing_oi_and_equity_metrics_never_receive_zero_defaults():
    unavailable_oi = open_interest()[1]
    unavailable_float = equity_metrics()[1]

    with pytest.raises(ValueError, match="must not contain a value"):
        replace(unavailable_oi, open_interest=0)
    with pytest.raises(ValueError, match="must not contain a default"):
        replace(unavailable_float, value=Decimal("0"))


def test_historical_volatility_requires_finalized_exact_bar_membership():
    bars = adjusted_bars()
    hv20 = volatility()[0]

    with pytest.raises(ValueError, match="adjusted daily bar identity"):
        replace(bars[-1], finalized=False)
    with pytest.raises(ValueError, match="membership or clocks"):
        replace(hv20, bars=hv20.bars[:-1])
    with pytest.raises(ValueError, match="membership or clocks"):
        replace(
            hv20,
            known_at=bars[-1].known_at - timedelta(days=1),
        )
    with pytest.raises(ValueError, match="does not match"):
        replace(
            hv20,
            annualized_volatility=hv20.annualized_volatility + Decimal("0.01"),
        )


def test_oi_adapter_reuses_exact_ref_baseline_identity_and_supersession():
    expected = open_interest()[0]
    fact = ReferenceFact.build(
        ReferenceFactRelation.OI_BASELINE,
        "OI_BASELINE",
        expected.contract_ticker,
        baseline_id=str(expected.baseline_id),
        contract_id=expected.contract_id,
        settlement_session=expected.settlement_session,
        open_interest=expected.open_interest,
        source=expected.source,
        date_basis=expected.date_basis,
        observed_session=expected.observed_session,
        observed_at=expected.observed_at,
        received_at=expected.received_at,
        payload_sha256=expected.payload_sha256,
        supersedes_baseline_id=None,
    )

    assert open_interest_reference_from_fact(
        fact,
        contract_ticker=expected.contract_ticker,
        underlyer=expected.underlyer,
        known_at=expected.known_at,
    ) == expected


def test_valuation_adapter_binds_existing_rate_and_dividend_inputs():
    expected = valuation()[0]
    curve = tuple(
        RiskFreeRateObservation(
            rate_observation_id=row.observation_id,
            rate_date=row.rate_date,
            tenor_days=row.tenor_days,
            annual_rate=float(row.annual_rate),
            source=row.source,
            source_key=row.source_key,
            source_observed_at=row.observed_at,
            first_observed_at=row.received_at,
            availability_mode="LIVE_OBSERVED",
            replay_available_at=None,
            payload_sha256=row.payload_sha256,
        )
        for row in expected.rate_observations
    )
    window = DividendReferenceWindow(
        security_id=SECURITY,
        ticker="SPY",
        window_start=SESSION,
        window_end=EXPIRATION,
        known_by=KNOWN,
        status="KNOWN_NO_ANNOUNCED_DIVIDEND",
        coverage_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        coverage_observed_at=KNOWN - timedelta(minutes=21),
        coverage_payload_sha256="5" * 64,
        response_sha256="6" * 64,
        checked_at=KNOWN - timedelta(minutes=20),
        future_coverage_complete=True,
    )
    coverage = dividend_coverage_reference_from_window(window)
    candidate = ExpirationValuationCandidate(
        valuation_date=SESSION,
        expiration_date=EXPIRATION,
        spot=Decimal("500"),
        risk_free_rate=float(expected.risk_free_rate),
        dividend_yield=0.0,
        rate_date=date(2026, 10, 15),
        rate_observation_ids=tuple(
            row.rate_observation_id for row in curve
        ),
        rate_payload_sha256=tuple(row.payload_sha256 for row in curve),
        dividend_cash_flows=(),
        dividend_coverage_start=SESSION,
        dividend_coverage_end=EXPIRATION,
        dividend_source_key="SPY:DIVIDEND:COVERAGE",
        dividend_payload_sha256=coverage.payload_sha256,
        selected_at=KNOWN,
    )

    adapted = valuation_reference_from_candidate(
        candidate,
        security_id=SECURITY,
        ticker="SPY",
        curve=curve,
        dividend_coverage=coverage,
        model_version=expected.model_version,
        model_bundle_sha256=expected.model_bundle_sha256,
        valuation_policy_sha256=expected.valuation_policy_sha256,
    )

    assert adapted.rate_observations == expected.rate_observations
    assert adapted.dividend_coverage == coverage
    with pytest.raises(ValueError, match="provenance changed"):
        valuation_reference_from_candidate(
            replace(candidate, rate_payload_sha256=("0" * 64,) * 2),
            security_id=SECURITY,
            ticker="SPY",
            curve=curve,
            dividend_coverage=coverage,
            model_version=expected.model_version,
            model_bundle_sha256=expected.model_bundle_sha256,
            valuation_policy_sha256=expected.valuation_policy_sha256,
        )


def test_tier_a_membership_is_complete_for_every_underlyer():
    state = reference_state()

    with pytest.raises(ValueError, match="HV20/HV30"):
        replace(
            state,
            historical_volatility=(
                state.historical_volatility[0],
            ),
        )
    with pytest.raises(ValueError, match="causal equity metric"):
        replace(state, equity_metrics=state.equity_metrics[:-1])


def test_corrected_fact_requires_a_new_generation_and_identity():
    original = reference_state()
    corrected = reference_state(
        generation=8,
        short_interest=Decimal("0.09"),
    )

    assert corrected.reference_generation == 8
    assert corrected.state_id != original.state_id
    assert corrected.state_sha256 != original.state_sha256
    assert corrected.member_identities != original.member_identities


def test_repository_is_idempotent_and_freezes_consumed_session_state():
    repository = InMemoryAdvancedOptionsReferenceStateRepository()
    original = reference_state()
    divergent_same_generation = reference_state(
        short_interest=Decimal("0.09"),
    )
    corrected = reference_state(
        generation=8,
        short_interest=Decimal("0.09"),
    )
    cycle = UUID("77777777-7777-7777-7777-777777777777")

    assert repository.put(original) is original
    assert repository.put(original) is original
    with pytest.raises(ValueError, match="generation is immutable"):
        repository.put(divergent_same_generation)
    repository.freeze_for_cycle(
        original,
        cycle_id=cycle,
        consumed_at=KNOWN,
    )
    repository.put(corrected)

    assert repository.load_generation(8) == corrected
    assert repository.load_for_session(SESSION, known_at=KNOWN) == original
    with pytest.raises(ValueError, match="session reference state is frozen"):
        repository.freeze_for_cycle(
            corrected,
            cycle_id=UUID("88888888-8888-8888-8888-888888888888"),
            consumed_at=KNOWN,
        )


def test_repository_rejects_noncausal_or_expired_consumption():
    repository = InMemoryAdvancedOptionsReferenceStateRepository()
    state = repository.put(reference_state())

    with pytest.raises(ValueError, match="cannot be consumed"):
        repository.freeze_for_cycle(
            state,
            cycle_id=UUID("99999999-9999-9999-9999-999999999999"),
            consumed_at=KNOWN - timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="cannot be consumed"):
        repository.freeze_for_cycle(
            state,
            cycle_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            consumed_at=state.valid_until,
        )


def test_available_equity_metric_rejects_future_observations():
    metric = equity_metrics()[0]

    with pytest.raises(ValueError, match="clocks are not causal"):
        AdvancedOptionsEquityMetric(
            security_id=metric.security_id,
            ticker=metric.ticker,
            metric_kind=metric.metric_kind,
            as_of_session=metric.as_of_session,
            availability=metric.availability,
            known_at=metric.known_at,
            value=metric.value,
            source=metric.source,
            source_key=metric.source_key,
            observed_at=metric.known_at + timedelta(seconds=1),
            received_at=metric.known_at + timedelta(seconds=1),
            payload_sha256=metric.payload_sha256,
        )
