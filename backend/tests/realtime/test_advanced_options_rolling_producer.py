from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from psycopg2 import Error as PsycopgError

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from realtime.advanced_options_reference_state import (
    REFERENCE_STATE_VERSION_V1,
)
from realtime.advanced_options_rolling_producer import (
    AdvancedOptionsEquityLiveHeadBinding,
    AdvancedOptionsRollingProducerPreflight,
    PostgresAdvancedOptionsRollingProducer,
    RollingProducerStatus,
    advanced_options_rolling_producer_plan_payload,
    build_advanced_options_rolling_producer_plan,
    _equity_live_head_source,
    _market_state,
)
from realtime.database_access_v2 import V2ConnectionProvider
from realtime.option_source_flow import build_option_source_flow_identity
from tests.realtime.test_advanced_options_reference_producer import (
    _contract_scope,
    _mixed_enrollment,
    _mixed_reference_state,
)
from tests.realtime.test_advanced_options_control_plane import (
    EXPIRES,
    PLANNED,
    _enrollment_plan,
)


def _plan(**changes):
    enrollment = changes.pop("enrollment", _enrollment_plan())
    scope = changes.pop("contract_scope", _contract_scope(enrollment))
    control = enrollment.control_plane
    source_flow = changes.pop(
        "source_flow",
        build_option_source_flow_identity(
            journal_generation=control.source_flow.journal_generation,
            stream_session_ids=control.source_flow.stream_session_ids,
            condition_semantics_sha256=(
                control.pins.condition_semantics_sha256
            ),
            flow_policy_sha256="d" * 64,
            advanced_flow_policy_sha256=scope.flow_policy_sha256,
            contract_scope_sha256=scope.sha256,
        ),
    )
    values = dict(
        enrollment=enrollment,
        contract_scope=scope,
        reference_state=reference_state(
            generation=1,
            known_at=PLANNED,
            valid_until=EXPIRES,
            data_membership_sha256=(
                enrollment.activity_selection.selection_sha256
            ),
            contract_scope_sha256=scope.sha256,
        ),
        source_flow=source_flow,
        owner_epoch=3,
        market_state_committed_offset=9,
        flow_committed_offset=11,
        market_state_shard_ids=(0,),
        flow_shard_ids=(0,),
        market_state_shard_map_sha256="e" * 64,
        flow_shard_map_sha256="f" * 64,
        known_at=PLANNED,
        valid_until=EXPIRES,
        maximum_underlying_age_ms=1_000,
        maximum_option_spot_skew_ms=500,
        maximum_cluster_members=10_000,
    )
    values.update(changes)
    return build_advanced_options_rolling_producer_plan(**values)


def test_rolling_producer_plan_freezes_scope_clocks_and_zero_authority():
    first = _plan()
    repeated = _plan()
    payload = advanced_options_rolling_producer_plan_payload(first)

    assert first == repeated
    assert payload == advanced_options_rolling_producer_plan_payload(
        repeated
    )
    assert payload["mode"] == "PLAN"
    assert payload["identity"]["cohort_id"] == first.control_plane.cohort_id
    assert payload["identity"]["reference_state_sha256"] == (
        first.reference_state.state_sha256
    )
    assert payload["identity"]["source_flow_sha256"] == (
        first.source_flow.source_flow_sha256
    )
    assert payload["identity"]["market_state_shard_map_sha256"] != (
        payload["identity"]["flow_shard_map_sha256"]
    )
    assert payload["identity"]["market_state_committed_offset"] == 9
    assert payload["identity"]["flow_committed_offset"] == 11
    assert set(payload["effect_counts"].values()) == {0}
    assert first.provider_calls_authorized is False
    assert first.runtime_actions_authorized is False


def test_rolling_plan_separates_data_scope_and_consumer_shards():
    enrollment = _mixed_enrollment()
    scope = _contract_scope(enrollment)
    plan = _plan(
        enrollment=enrollment,
        contract_scope=scope,
        reference_state=_mixed_reference_state(
            data_membership_sha256=(
                enrollment.activity_selection.selection_sha256
            ),
            contract_scope_sha256=scope.sha256,
            known_at=PLANNED,
            valid_until=EXPIRES,
        ),
        market_state_shard_ids=(0, 1),
        flow_shard_ids=(0,),
    )

    assert plan.data_membership_sha256 == (
        enrollment.activity_selection.selection_sha256
    )
    assert plan.market_state_shard_ids == (0, 1)
    assert plan.flow_shard_ids == (0,)
    assert plan.market_state_committed_offset == 9
    assert plan.flow_committed_offset == 11
    with pytest.raises(
        ValueError,
        match="clocks, bounds or authority changed",
    ):
        _plan(
            enrollment=enrollment,
            contract_scope=plan.contract_scope,
            reference_state=plan.reference_state,
            flow_shard_ids=(),
        )


def test_rolling_plan_accepts_immutable_v1_reference_for_exact_membership():
    enrollment = _enrollment_plan()
    scope = _contract_scope(enrollment)
    legacy = reference_state(
        generation=1,
        known_at=PLANNED,
        valid_until=EXPIRES,
        data_membership_sha256=None,
        contract_scope_sha256=scope.sha256,
    )

    plan = _plan(
        enrollment=enrollment,
        contract_scope=scope,
        reference_state=legacy,
    )

    assert plan.reference_state.version == REFERENCE_STATE_VERSION_V1


def test_rolling_producer_plan_rejects_divergence_and_stale_bounds():
    plan = _plan()
    with pytest.raises(ValueError, match="plan identity changed"):
        advanced_options_rolling_producer_plan_payload(
            replace(plan, plan_sha256="0" * 64)
        )
    with pytest.raises(ValueError, match="source-flow identity changed"):
        _plan(source_flow=build_option_source_flow_identity(
            journal_generation=uuid4(),
            stream_session_ids=plan.source_flow.stream_session_ids,
            condition_semantics_sha256=(
                plan.source_flow.condition_semantics_sha256
            ),
            flow_policy_sha256=plan.source_flow.flow_policy_sha256,
            advanced_flow_policy_sha256=(
                plan.source_flow.advanced_flow_policy_sha256
            ),
            contract_scope_sha256=plan.source_flow.contract_scope_sha256,
        ))
    with pytest.raises(ValueError, match="clocks, bounds or authority"):
        _plan(maximum_cluster_members=0)


def _equity_binding(**changes):
    values = dict(
        cohort_id=UUID("90000000-0000-0000-0000-000000000001"),
        manifest_id=UUID("90000000-0000-0000-0000-000000000002"),
        manifest_sha256="1" * 64,
        ingress_plan_id=UUID(
            "90000000-0000-0000-0000-000000000003"
        ),
        ingress_plan_sha256="2" * 64,
        journal_generation=UUID(
            "90000000-0000-0000-0000-000000000004"
        ),
        stream_session_ids=(
            UUID("90000000-0000-0000-0000-000000000005"),
        ),
        runtime_profile_sha256="3" * 64,
        configuration_sha256="4" * 64,
        runtime_policy_sha256="5" * 64,
        journal_metadata_sha256="6" * 64,
        shard_map_sha256="7" * 64,
        session_date=PLANNED.date(),
        committed_offset=20,
        observed_at=PLANNED,
        valid_until=EXPIRES,
    )
    values.update(changes)
    return AdvancedOptionsEquityLiveHeadBinding(**values)


def _head(
    kind,
    instrument_id,
    generation,
    session_id,
    event_at,
):
    quote = kind == "Q"
    return {
        "kind": kind,
        "instrument_id": instrument_id,
        "source_session": PLANNED.date(),
        "source_sequence": 1,
        "event_time_raw": int(event_at.timestamp() * 1000),
        "event_time_unit": "ms",
        "event_at": event_at,
        "received_at": event_at,
        "bid": Decimal("99") if quote else None,
        "ask": Decimal("101") if quote else None,
        "bid_size": 10 if quote else None,
        "ask_size": 12 if quote else None,
        "bid_exchange": 301 if quote else None,
        "ask_exchange": 302 if quote else None,
        "quality": "GOOD" if quote else None,
        "reason_codes": [],
        "session_id": str(session_id),
        "journal_generation": str(generation),
        "shard_id": 0,
        "source_offset": 10,
        "payload_sha256": "8" * 64,
        "price": None if quote else Decimal("100"),
        "size": None if quote else Decimal("5"),
        "exchange": None if quote else 302,
        "conditions": [] if not quote else None,
        "eligibility": None if quote else "INCLUDED",
        "eligibility_policy_version": None if quote else "live-v1",
    }


class _HeadCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement, _parameters):
        return None

    def fetchall(self):
        return self.rows


def _separate_head_inputs(plan, binding):
    selected = plan.enrollment.activity_selection.contracts[0]
    security_id = dict(
        plan.enrollment.equity_plan.security_ids
    )[selected.underlyer]
    option_id = selected.instrument_id
    equity_id = option_id + 100_000
    at = PLANNED - timedelta(milliseconds=100)
    rows = (
        _head(
            "Q",
            option_id,
            plan.source_flow.journal_generation,
            plan.source_flow.stream_session_ids[0],
            at,
        ),
        _head(
            "Q",
            equity_id,
            binding.journal_generation,
            binding.stream_session_ids[0],
            at,
        ),
        _head(
            "T",
            option_id,
            plan.source_flow.journal_generation,
            plan.source_flow.stream_session_ids[0],
            at,
        ),
        _head(
            "T",
            equity_id,
            binding.journal_generation,
            binding.stream_session_ids[0],
            at,
        ),
    )
    return (
        rows,
        ({
            "instrument_id": option_id,
            "underlying_security_id": str(security_id),
        },),
        ({
            "instrument_id": equity_id,
            "security_id": str(security_id),
            "ticker": selected.underlyer,
        },),
    )


def test_market_state_accepts_separate_frozen_equity_and_options_identities():
    binding = _equity_binding()
    plan = _plan(equity_live_head_binding=binding)
    rows, contracts, equities = _separate_head_inputs(plan, binding)
    evidence = []

    result = _market_state(
        _HeadCursor(rows),
        plan,
        contracts,
        equities,
        evidence,
    )

    assert result is not None
    assert result[0][0].quote.journal_generation == (
        binding.journal_generation
    )
    assert evidence[-1].availability.value == "READY"


def test_market_state_refuses_missing_and_cross_swapped_head_identities():
    binding = _equity_binding()
    plan = _plan(equity_live_head_binding=binding)
    rows, contracts, equities = _separate_head_inputs(plan, binding)
    evidence = []
    assert _market_state(
        _HeadCursor(rows[:-1]),
        plan,
        contracts,
        equities,
        evidence,
    ) is None
    assert "CANONICAL_CURRENT_HEADS_INCOMPLETE" in (
        evidence[-1].reason_codes
    )
    swapped = list(rows)
    swapped[1] = {
        **swapped[1],
        "journal_generation": str(
            plan.source_flow.journal_generation
        ),
        "session_id": str(plan.source_flow.stream_session_ids[0]),
    }
    with pytest.raises(ValueError, match="generation drifted"):
        _market_state(
            _HeadCursor(tuple(swapped)),
            plan,
            contracts,
            equities,
            [],
        )


def test_rolling_plan_refuses_stale_equity_binding():
    stale = _equity_binding(
        observed_at=PLANNED - timedelta(seconds=2),
        valid_until=PLANNED,
    )
    with pytest.raises(ValueError, match="Equity live-head binding"):
        _plan(equity_live_head_binding=stale)


def _source_row(binding):
    return {
        "plan_sha256": binding.ingress_plan_sha256,
        "cohort_id": str(binding.cohort_id),
        "manifest_id": str(binding.manifest_id),
        "manifest_sha256": binding.manifest_sha256,
        "session_date": binding.session_date,
        "runtime_profile_sha256": binding.runtime_profile_sha256,
        "configuration_sha256": binding.configuration_sha256,
        "runtime_policy_sha256": binding.runtime_policy_sha256,
        "authorized_end_at": binding.valid_until,
        "status": "ACTIVE",
        "journal_generation": str(binding.journal_generation),
        "stream_session_id": str(binding.stream_session_ids[0]),
        "metadata_sha256": binding.journal_metadata_sha256,
        "shard_map_sha256": binding.shard_map_sha256,
        "bridge_committed_offset": binding.committed_offset,
        "registry_status": "WRITING",
        "heartbeat_at": binding.observed_at,
    }


def test_equity_source_binding_refuses_missing_or_policy_drift():
    binding = _equity_binding()
    plan = _plan(equity_live_head_binding=binding)
    evidence = []
    _equity_live_head_source(
        _HeadCursor((_source_row(binding),)),
        plan,
        evidence,
    )
    assert evidence[-1].availability.value == "READY"
    missing = []
    _equity_live_head_source(
        _HeadCursor(()),
        plan,
        missing,
    )
    assert missing[-1].reason_codes == (
        "EQUITY_LIVE_HEAD_BINDING_MISSING",
    )
    drifted = {
        **_source_row(binding),
        "runtime_policy_sha256": "f" * 64,
    }
    with pytest.raises(ValueError, match="source binding drifted"):
        _equity_live_head_source(
            _HeadCursor((drifted,)),
            plan,
            [],
        )


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


def test_rolling_producer_retries_one_serialization_failure(monkeypatch):
    provider = _RetryProvider()
    waits = []
    plan = _plan()
    producer = PostgresAdvancedOptionsRollingProducer(
        provider,
        plan,
        maximum_attempts=2,
        retry_initial_seconds=0.01,
        wait=waits.append,
    )
    state = rolling_state(
        cohort_id=plan.control_plane.cohort_id,
        journal_generation=plan.source_flow.journal_generation,
    )
    result = AdvancedOptionsRollingProducerPreflight(
        status=RollingProducerStatus.READY,
        plan_id=plan.plan_id,
        preflight_sha256="a" * 64,
        evidence=(),
        state=state,
    )
    monkeypatch.setattr(producer, "_assemble", lambda _unit: result)
    monkeypatch.setattr(
        "realtime.advanced_options_rolling_producer."
        "persist_advanced_options_rolling_state_cursor",
        lambda _cursor, value: value,
    )

    assert producer.produce() == state
    assert provider.attempts == 2
    assert waits == [0.01]
