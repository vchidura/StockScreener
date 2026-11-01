from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from psycopg2 import Error as PsycopgError

from advanced_options_reference_support import reference_state
from realtime.advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
    FlowDteLane,
    FlowUniverseTier,
)
from realtime.advanced_options_reference_producer import (
    AdvancedOptionsReferenceProducerPreflight,
    PostgresAdvancedOptionsReferenceProducer,
    ReferenceProducerStatus,
    _coalesce_terminal_open_interest,
    advanced_options_reference_producer_plan_payload,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_reference_state import (
    build_advanced_options_reference_state,
)
from realtime.contracts import MarketChannel
from realtime.advanced_equity_control_plane import (
    build_advanced_equity_control_plane_plan,
)
from realtime.database_access_v2 import V2ConnectionProvider
from realtime.option_session_manifest import (
    OptionActivityContract,
    OptionActivitySelectionRecord,
)
from realtime.option_slot_set import SlotSetMember, SlotSetPurpose
from tests.realtime.test_advanced_options_control_plane import (
    CHECKED,
    EXPIRES,
    PLANNED,
    SESSION,
    _manifest_policy,
    _pins,
    _plan as _control_plan,
    _receipt_inputs,
    _enrollment_plan,
)


def _contract_scope(enrollment, **changes):
    values = dict(
        version="advanced-options-contract-scope-v1",
        session_date=enrollment.control_plane.session_date,
        manifest_sha256=enrollment.control_plane.manifest_sha256,
        slot_sha256=enrollment.control_plane.slot_set_sha256,
        flow_policy_sha256=(
            enrollment.control_plane.pins.flow_policy_sha256
        ),
        contracts=(
            AdvancedOptionsContractScopeEntry(
                contract_ticker="O:SPY261120C00500000",
                underlyer="SPY",
                expiration=date(2026, 11, 20),
                universe_tier=FlowUniverseTier.ETF,
                dte_lane=FlowDteLane.CORE_SWING,
            ),
        ),
    )
    values.update(changes)
    return AdvancedOptionsContractScope(**values)


def _mixed_reference_state(
    *,
    data_membership_sha256="1" * 64,
    contract_scope_sha256="0" * 64,
    known_at=None,
    valid_until=None,
):
    default_reference = reference_state()
    base_reference = reference_state(
        known_at=known_at or default_reference.known_at,
        valid_until=valid_until or default_reference.valid_until,
        data_membership_sha256=data_membership_sha256,
        contract_scope_sha256=contract_scope_sha256,
    )
    amd_security = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    amd_volatility = tuple(
        replace(
            fact,
            security_id=amd_security,
            ticker="AMD",
            bars=tuple(
                replace(
                    bar,
                    security_id=amd_security,
                    ticker="AMD",
                )
                for bar in fact.bars
            ),
        )
        for fact in base_reference.historical_volatility
    )
    amd_valuations = tuple(
        replace(
            value,
            security_id=amd_security,
            ticker="AMD",
            dividend_coverage=replace(
                value.dividend_coverage,
                security_id=amd_security,
                ticker="AMD",
            ),
        )
        for value in base_reference.valuation_inputs
    )
    amd_metrics = tuple(
        replace(
            value,
            security_id=amd_security,
            ticker="AMD",
            source_key=(
                None
                if value.source_key is None
                else value.source_key.replace("SPY", "AMD")
            ),
        )
        for value in base_reference.equity_metrics
    )
    return build_advanced_options_reference_state(
        reference_generation=base_reference.reference_generation,
        session_date=base_reference.session_date,
        known_at=base_reference.known_at,
        valid_until=base_reference.valid_until,
        reference_policy_sha256=base_reference.reference_policy_sha256,
        data_membership_sha256=base_reference.data_membership_sha256,
        contract_scope_sha256=base_reference.contract_scope_sha256,
        underlyings=("AMD", "SPY"),
        open_interest=(
            *base_reference.open_interest,
            replace(
                base_reference.open_interest[-1],
                contract_id=2_001,
                contract_ticker="O:AMD261019C00100000",
                underlyer="AMD",
            ),
        ),
        historical_volatility=(
            *base_reference.historical_volatility,
            *amd_volatility,
        ),
        valuation_inputs=(
            *base_reference.valuation_inputs,
            *amd_valuations,
        ),
        equity_metrics=(
            *base_reference.equity_metrics,
            *amd_metrics,
        ),
    )


def _mixed_enrollment(*, flow_policy_sha256="2" * 64):
    mixed_reference = _mixed_reference_state()
    equity = build_advanced_equity_control_plane_plan(
        session_date=SESSION,
        security_ids={
            "AAPL": UUID(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            ),
            "AMD": UUID(
                "cccccccc-cccc-cccc-cccc-cccccccccccc"
            ),
            "SPY": UUID(
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            ),
        },
        entitlement_evidence_sha256="a" * 64,
        authorization_reference="advanced-equity-v2-20261016",
        checked_at=CHECKED,
        expires_at=EXPIRES,
        schema_sha256="b" * 64,
        security_policy_sha256="c" * 64,
    )
    members = tuple(
        SlotSetMember(
            instrument_id,
            channel,
            SlotSetPurpose.PRIMARY,
            100,
            instrument_id + 1_000,
        )
        for instrument_id in (101, 201, 202)
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    control = _control_plan(
        equity_plan=equity,
        option_underlyings=("AMD", "SPY"),
        manifest_policy=_manifest_policy(
            maximum_option_quote_contracts=3,
            reserved_option_quote_contracts=0,
        ),
        pins=_pins(flow_policy_sha256=flow_policy_sha256),
        slot_members=members,
        receipt_inputs=_receipt_inputs(
            reference_state=mixed_reference,
        ),
    )
    base = _enrollment_plan().activity_selection.contracts[0]
    contracts = (
        base,
        OptionActivityContract(
            instrument_id=201,
            contract_id=2_001,
            catalog_version_id=1_201,
            contract_ticker="O:AMD261019C00100000",
            underlyer="AMD",
            call_put="C",
            expiration_date=date(2026, 10, 19),
            strike=Decimal("100"),
            exercise_style="AMERICAN",
            multiplier=Decimal("100"),
            deliverables=(),
            eligibility="VALIDATED_ACTIVE",
            catalog_effective_from=CHECKED,
            catalog_effective_to=None,
            catalog_observed_at=PLANNED - timedelta(minutes=3),
            catalog_payload_sha256="5" * 64,
            market_data_time=PLANNED - timedelta(minutes=2),
            source_sha256="4" * 64,
        ),
        OptionActivityContract(
            instrument_id=202,
            contract_id=2_002,
            catalog_version_id=1_202,
            contract_ticker="O:AMD261020P00095000",
            underlyer="AMD",
            call_put="P",
            expiration_date=date(2026, 10, 20),
            strike=Decimal("95"),
            exercise_style="AMERICAN",
            multiplier=Decimal("100"),
            deliverables=(),
            eligibility="VALIDATED_ACTIVE",
            catalog_effective_from=CHECKED,
            catalog_effective_to=None,
            catalog_observed_at=PLANNED - timedelta(minutes=3),
            catalog_payload_sha256="3" * 64,
            market_data_time=PLANNED - timedelta(minutes=2),
            source_sha256="2" * 64,
        ),
    )
    selection = OptionActivitySelectionRecord(
        authorization_reference="mixed-flow-selection",
        reviewed_at=PLANNED - timedelta(minutes=1),
        valid_until=PLANNED + timedelta(minutes=2),
        policy_sha256="9" * 64,
        source_sha256="8" * 64,
        quote_ceiling=3,
        contracts=contracts,
    )
    return _enrollment_plan(
        equity_plan=equity,
        control_plane=control,
        activity_selection=selection,
    )


def _plan(**changes):
    enrollment = changes.pop("enrollment", _enrollment_plan())
    values = dict(
        enrollment=enrollment,
        contract_scope=_contract_scope(enrollment),
        reference_generation=1,
        known_at=PLANNED,
        valid_until=EXPIRES,
        discovery_scheduled_cycle=PLANNED,
        prior_oi_session=date(2026, 10, 15),
        model_version="black-scholes-merton-v1",
    )
    values.update(changes)
    return build_advanced_options_reference_producer_plan(**values)


def test_reference_producer_plan_freezes_scope_clocks_and_zero_authority():
    first = _plan()
    repeated = _plan()
    payload = advanced_options_reference_producer_plan_payload(first)

    assert first == repeated
    assert payload == advanced_options_reference_producer_plan_payload(
        repeated
    )
    assert payload["identity"]["cohort_id"] == str(
        first.control_plane.cohort_id
    )
    assert payload["identity"]["slot_set_sha256"] == (
        first.control_plane.slot_set_sha256
    )
    assert payload["identity"]["contract_scope_sha256"] == (
        first.contract_scope.sha256
    )
    assert set(payload["effect_counts"].values()) == {0}
    assert first.provider_calls_authorized is False
    assert first.runtime_actions_authorized is False


def test_reference_plan_binds_broad_data_and_narrow_flow_membership():
    enrollment = _mixed_enrollment()
    plan = _plan(
        enrollment=enrollment,
        contract_scope=_contract_scope(enrollment),
    )

    assert plan.data_membership_sha256 == (
        enrollment.activity_selection.selection_sha256
    )
    assert tuple(plan.contract_scope.contract_map) == (
        "O:SPY261120C00500000",
    )
    assert {
        row.contract_ticker
        for row in enrollment.activity_selection.contracts
    } == {
        "O:AMD261019C00100000",
        "O:AMD261020P00095000",
        "O:SPY261120C00500000",
    }
    with pytest.raises(ValueError, match="contract scope changed"):
        _plan(
            enrollment=enrollment,
            contract_scope=replace(
                plan.contract_scope,
                contracts=(
                    *plan.contract_scope.contracts,
                    AdvancedOptionsContractScopeEntry(
                        contract_ticker="O:SPY261120P00400000",
                        underlyer="SPY",
                        expiration=date(2026, 11, 20),
                        universe_tier=FlowUniverseTier.ETF,
                        dte_lane=FlowDteLane.CORE_SWING,
                    ),
                ),
            ),
        )


def test_identical_terminal_oi_observations_coalesce_to_latest_clock():
    base = {
        "baseline_id": "11111111-1111-1111-1111-111111111111",
        "contract_id": 7,
        "settlement_session": date(2026, 10, 26),
        "open_interest": 123,
        "source": "massive",
        "date_basis": "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
        "observed_session": date(2026, 10, 27),
        "observed_at": PLANNED,
        "received_at": PLANNED,
        "payload_sha256": "a" * 64,
        "supersedes_baseline_id": None,
    }
    repeated = {
        **base,
        "baseline_id": "22222222-2222-2222-2222-222222222222",
        "observed_at": PLANNED + timedelta(seconds=1),
        "received_at": PLANNED + timedelta(seconds=1),
        "payload_sha256": "b" * 64,
    }

    (selected,) = _coalesce_terminal_open_interest(
        (base, repeated),
        date(2026, 10, 27),
    )

    assert selected["baseline_id"] == repeated["baseline_id"]
    divergent = {**repeated, "open_interest": 124}
    assert len(_coalesce_terminal_open_interest(
        (base, divergent),
        date(2026, 10, 27),
    )) == 2


def test_reference_producer_plan_rejects_divergence_scope_and_stale_clocks():
    plan = _plan()
    with pytest.raises(ValueError, match="plan identity changed"):
        advanced_options_reference_producer_plan_payload(
            replace(plan, plan_sha256="0" * 64)
        )
    with pytest.raises(ValueError, match="contract scope changed"):
        _plan(contract_scope=replace(
            plan.contract_scope,
            slot_sha256="1" * 64,
        ))
    with pytest.raises(ValueError, match="clocks or authority changed"):
        _plan(known_at=PLANNED - timedelta(seconds=1))


class _RetryableError(PsycopgError):
    pgcode = "40001"


class _RetryProvider(V2ConnectionProvider):
    def __init__(self):
        self.attempts = 0

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.attempts += 1
        if self.attempts == 1:
            raise _RetryableError("retry")
        yield SimpleNamespace(cursor=object(), connection=object())


def test_reference_producer_retries_one_serialization_failure(
    monkeypatch,
):
    provider = _RetryProvider()
    waits = []
    producer = PostgresAdvancedOptionsReferenceProducer(
        provider,
        _plan(),
        maximum_attempts=2,
        retry_initial_seconds=0.01,
        wait=waits.append,
    )
    state = reference_state()
    result = AdvancedOptionsReferenceProducerPreflight(
        status=ReferenceProducerStatus.READY,
        plan_id=producer._plan.plan_id,
        preflight_sha256="a" * 64,
        evidence=(),
        state=state,
    )
    monkeypatch.setattr(producer, "_assemble", lambda _unit: result)
    monkeypatch.setattr(
        "realtime.advanced_options_reference_producer."
        "persist_advanced_options_reference_state_cursor",
        lambda _cursor, value: value,
    )

    assert producer.produce() == state
    assert provider.attempts == 2
    assert waits == [0.01]
