from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from psycopg2 import Error as PsycopgError

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from options.config import load_valuation_policy
from realtime.advanced_options_mark_producer import (
    AdvancedOptionsMarkPreflight,
    AdvancedOptionsMarkStatus,
    PostgresAdvancedOptionsMarkProducer,
    advanced_options_mark_producer_plan_payload,
    assemble_advanced_options_marks_cursor,
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


class _QuoteCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement, _parameters):
        return None

    def fetchall(self):
        return self.rows


def _quote_rows(plan, *, event_at, received_at):
    control = plan.control_plane
    return tuple(
        {
            "instrument_id": row.instrument_id,
            "catalog_version_id": row.catalog_version_id,
            "contract_id": row.contract_id,
            "contract_ticker": row.contract_ticker,
            "underlying_security_id": str(uuid4()),
            "expiration": control.session_date,
            "source_session": control.session_date,
            "source_sequence": 1,
            "event_time_raw": int(event_at.timestamp() * 1000),
            "event_time_unit": "ms",
            "event_at": event_at,
            "received_at": received_at,
            "bid": Decimal("1"),
            "ask": Decimal("1.1"),
            "bid_size": 10,
            "ask_size": 12,
            "bid_exchange": 301,
            "ask_exchange": 302,
            "quality": "GOOD",
            "reason_codes": [],
            "session_id": str(plan.source_flow.stream_session_ids[0]),
            "journal_generation": str(plan.source_flow.journal_generation),
            "shard_id": 0,
            "source_offset": 1,
            "payload_sha256": "9" * 64,
        }
        for row in plan.enrollment.activity_selection.contracts
    )


def _noncausal_rows(result):
    return [
        row
        for row in result.evidence
        if "OPTION_NBBO_STALE_OR_NONCAUSAL" in row.reason_codes
    ]


def test_quote_from_before_the_cutoff_may_arrive_just_after_it():
    plan = _plan()

    result = assemble_advanced_options_marks_cursor(
        _QuoteCursor(_quote_rows(
            plan,
            event_at=NOW - timedelta(milliseconds=2),
            received_at=NOW + timedelta(milliseconds=40),
        )),
        plan,
    )

    assert _noncausal_rows(result) == []


def test_quote_from_after_the_cutoff_stays_out_of_the_window():
    plan = _plan()

    result = assemble_advanced_options_marks_cursor(
        _QuoteCursor(_quote_rows(
            plan,
            event_at=NOW + timedelta(milliseconds=1),
            received_at=NOW + timedelta(milliseconds=40),
        )),
        plan,
    )

    assert _noncausal_rows(result)


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


def test_quote_stamped_received_before_its_event_by_clock_skew_is_marked():
    """R34 (Nov 10 live): the host clock was 0.4-0.6 s behind the exchange, so quotes were received "before" their
    event and every contract was left out as NONCAUSAL; O1 built no package for the rest of the session. Up to 2 s of
    skew is taken as received at the event time; more is still noncausal."""
    plan = _plan()
    skewed = assemble_advanced_options_marks_cursor(
        _QuoteCursor(_quote_rows(
            plan,
            event_at=NOW - timedelta(milliseconds=100),
            received_at=NOW - timedelta(milliseconds=550),
        )),
        plan,
    )
    assert _noncausal_rows(skewed) == []
    assert all(mark.observed_time == mark.market_time == NOW - timedelta(milliseconds=100) for mark in skewed.marks)
    far = assemble_advanced_options_marks_cursor(
        _QuoteCursor(_quote_rows(
            plan,
            event_at=NOW - timedelta(milliseconds=100),
            received_at=NOW - timedelta(seconds=3),
        )),
        plan,
    )
    assert _noncausal_rows(far)

