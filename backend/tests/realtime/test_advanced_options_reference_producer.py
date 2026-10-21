from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

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
    advanced_options_reference_producer_plan_payload,
    build_advanced_options_reference_producer_plan,
)
from realtime.database_access_v2 import V2ConnectionProvider
from tests.realtime.test_advanced_options_control_plane import (
    EXPIRES,
    PLANNED,
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
        treasury_maximum_age_seconds=3_600,
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
