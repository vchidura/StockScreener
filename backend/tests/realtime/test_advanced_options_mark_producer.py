from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from psycopg2 import Error as PsycopgError

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from options.config import load_valuation_policy
from realtime.advanced_options_mark_producer import (
    AdvancedOptionsMarkPreflight,
    AdvancedOptionsMarkStatus,
    PostgresAdvancedOptionsMarkProducer,
    advanced_options_mark_producer_plan_payload,
    build_advanced_options_mark_producer_plan,
    freeze_advanced_options_mark_producer_plan,
)
from realtime.advanced_options_rolling_state import (
    AdvancedOptionsRollingProducerBinding,
)
from realtime.database_access_v2 import V2ConnectionProvider
from realtime.option_source_flow import build_option_source_flow_identity
from test_advanced_options_control_plane import (
    _enrollment_plan,
    _manifest_policy,
    _pins,
    _plan as _control_plan,
    _slot_members,
)
from realtime.option_slot_set import SlotSetPurpose
from test_advanced_options_input import NOW


def _policy():
    return load_valuation_policy(
        Path(__file__).parents[2]
        / "options"
        / "policies"
        / "valuation_advanced_nbbo_v1.json"
    )


def _plan():
    policy = _policy()
    control = _control_plan(
        pins=replace(
            _pins(),
            valuation_policy_sha256=policy.policy_sha256,
        ),
        manifest_policy=_manifest_policy(
            reserved_option_quote_contracts=0
        ),
        slot_members=tuple(
            member
            for member in _slot_members()
            if member.purpose is SlotSetPurpose.PRIMARY
        ),
    )
    enrollment = _enrollment_plan(control_plane=control)
    reference = reference_state()
    source_flow = build_option_source_flow_identity(
        journal_generation=control.source_flow.journal_generation,
        stream_session_ids=control.source_flow.stream_session_ids,
        condition_semantics_sha256=control.pins.condition_semantics_sha256,
        flow_policy_sha256="1" * 64,
        advanced_flow_policy_sha256=control.pins.flow_policy_sha256,
        contract_scope_sha256=reference.contract_scope_sha256,
    )
    binding = AdvancedOptionsRollingProducerBinding(
        producer_plan_id=control.plan_id,
        producer_plan_sha256=control.plan_sha256,
        reference_state_id=reference.state_id,
        reference_state_sha256=reference.state_sha256,
        reference_generation=reference.reference_generation,
        source_flow_id=source_flow.source_flow_id,
        source_flow_sha256=source_flow.source_flow_sha256,
        known_at=NOW,
        valid_until=reference.valid_until,
        unresolved_gaps_sha256="2" * 64,
        source_watermarks_sha256="3" * 64,
        market_state_heads_sha256="4" * 64,
        volume_checkpoint_sha256="5" * 64,
    )
    rolling = rolling_state(
        cohort_id=control.cohort_id,
        journal_generation=source_flow.journal_generation,
        committed_at=NOW,
        producer_binding=binding,
    )
    return build_advanced_options_mark_producer_plan(
        enrollment=enrollment,
        reference_state=reference,
        rolling_state=rolling,
        source_flow=source_flow,
        cutoff=NOW,
        producer_build_id="advanced-options-nbbo-mark-v1",
        producer_build_sha256="6" * 64,
        valuation_policy=policy,
    )


def _preflight(plan):
    return AdvancedOptionsMarkPreflight(
        status=AdvancedOptionsMarkStatus.READY,
        plan_id=plan.plan_id,
        preflight_sha256="7" * 64,
        marks_sha256="8" * 64,
        marks=(),
        evidence=(),
    )


def test_mark_plan_freezes_exact_scope_policy_and_zero_authority():
    plan = _plan()
    payload = advanced_options_mark_producer_plan_payload(plan)
    frozen = freeze_advanced_options_mark_producer_plan(
        plan,
        _preflight(plan),
    )

    assert payload["mode"] == "PLAN"
    assert payload["identity"]["cohort_id"] == plan.control_plane.cohort_id
    assert payload["identity"]["reference_state_sha256"] == (
        plan.reference_state.state_sha256
    )
    assert payload["identity"]["rolling_state_sha256"] == (
        plan.rolling_state.state_sha256
    )
    assert payload["identity"]["maximum_source_age_seconds"] == 1
    assert payload["identity"]["maximum_package_quote_skew_ms"] == 250
    assert set(payload["effect_counts"].values()) == {0}
    assert frozen.frozen
    assert frozen.expected_marks_sha256 == "8" * 64


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


def test_mark_producer_retries_serialization_rollback(monkeypatch):
    plan = _plan()
    preflight = _preflight(plan)
    frozen = freeze_advanced_options_mark_producer_plan(plan, preflight)
    provider = _RetryProvider()
    waits = []
    producer = PostgresAdvancedOptionsMarkProducer(
        provider,
        frozen,
        maximum_attempts=2,
        retry_initial_seconds=0.01,
        wait=waits.append,
    )
    monkeypatch.setattr(producer, "_assemble", lambda _unit: preflight)

    assert producer.produce() == ()
    assert provider.attempts == 2
    assert waits == [0.01]
