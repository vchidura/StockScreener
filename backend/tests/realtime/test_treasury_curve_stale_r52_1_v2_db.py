"""R52.1 (Nov 18) against the real schema: the reference producer on a stale-curve day stays READY, persists its state
and stores TREASURY_CURVE_STALE on the curve's evidence row; a missing curve still blocks; a normal day stores no code.

Fixture: the C6 mixed enrollment (session Fri Oct 16 2026; expected curve Oct 15, `treasury_rate_date_for_session`).
"""
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from option_flow_support import advanced_policy
from options.calendar import OptionExchangeCalendar
from options.model_inputs import treasury_rate_date_for_session
from realtime.advanced_options_control_plane import persist_advanced_options_enrollment_cursor
from realtime.advanced_options_input import AdvancedOptionsInputDependencyEvidence, AdvancedOptionsInputDependencyStatus
from realtime.advanced_options_reference_producer import (
    AdvancedOptionsReferencePreflightBlocked,
    PostgresAdvancedOptionsReferenceProducer,
    ReferenceProducerStatus,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_reference_state import ReferenceAvailability
from realtime.contracts import canonical_sha256
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.option_source_flow import persist_option_contract_scope_cursor, persist_option_flow_policy_cursor
from tests.realtime.test_advanced_options_reference_producer import _mixed_enrollment
from tests.realtime.test_advanced_options_reference_producer_v2_db import (
    _mixed_contract_scope,
    _persist_mixed_enrollment_inputs,
    _seed_reference_inputs,
)
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database


def _plan(enrollment, scope, generation):
    control = enrollment.control_plane
    return build_advanced_options_reference_producer_plan(
        enrollment=enrollment, contract_scope=scope, reference_generation=generation, known_at=control.planned_at,
        valid_until=control.expires_at, discovery_scheduled_cycle=control.planned_at,
        prior_oi_session=control.session_date - timedelta(days=1), model_version="black-scholes-merton-v1")


def _curve(database_cursor, plan, rate_date):
    known = plan.known_at
    for tenor, rate in ((30, Decimal("0.0400")), (60, Decimal("0.0410"))):
        database_cursor.execute(
            "INSERT INTO ref.risk_free_rate_observations (observation_id,rate_date,tenor_days,annual_rate,source,"
            "source_key,source_observed_at,received_at,payload_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (str(uuid4()), rate_date, tenor, rate, "us_treasury_par_yield_v1", f"{rate_date.isoformat()}:{tenor}d",
             known - timedelta(minutes=25), known - timedelta(minutes=20),
             canonical_sha256(("r52.1", rate_date.isoformat(), tenor))))


def _curve_row(result):
    (row,) = [row for row in result.evidence if row.member_kind == "TREASURY_CURVE"]
    return row


def test_a_stale_curve_day_is_ready_and_stores_the_advisory_code():
    with disposable_v2_database(f"test_realtime_v2_night_e_r52_{uuid4().hex[:8]}") as database_pool:
        flow_policy = advanced_policy()
        enrollment = _mixed_enrollment(flow_policy_sha256=flow_policy.sha256)
        scope = _mixed_contract_scope(enrollment, flow_policy)
        session = enrollment.control_plane.session_date
        expected = treasury_rate_date_for_session(session, OptionExchangeCalendar())
        stale = OptionExchangeCalendar().previous_session_date(expected)
        first, second = _plan(enrollment, scope, 1), _plan(enrollment, scope, 2)
        with cursor(database_pool) as database_cursor:
            _persist_mixed_enrollment_inputs(database_cursor, enrollment)
            persist_advanced_options_enrollment_cursor(database_cursor, enrollment)
            persist_option_flow_policy_cursor(database_cursor, flow_policy)
            persist_option_contract_scope_cursor(database_cursor, scope)
            for ticker in ("SPY", "AMD"):
                _seed_reference_inputs(database_cursor, enrollment, first, ticker=ticker, include_treasury=False)
        provider = V2ConnectionProvider({OWNER_POOL: database_pool}, V2AccessConfig(allow_disposable=True))

        # No curve at all: still BLOCKED with TREASURY_CURVE_MISSING (R52 D).
        missing = PostgresAdvancedOptionsReferenceProducer(provider, first, retry_initial_seconds=0).preflight()
        assert missing.status is ReferenceProducerStatus.BLOCKED
        assert "TREASURY_CURVE_MISSING" in missing.reason_codes
        with pytest.raises(AdvancedOptionsReferencePreflightBlocked):
            PostgresAdvancedOptionsReferenceProducer(provider, first, retry_initial_seconds=0).produce()

        # Only the curve one session older than expected: READY, the code stored, no blocker reported.
        with cursor(database_pool) as database_cursor:
            _curve(database_cursor, first, stale)
        producer = PostgresAdvancedOptionsReferenceProducer(provider, first, retry_initial_seconds=0)
        preflight = producer.preflight()
        assert preflight.status is ReferenceProducerStatus.READY and preflight.reason_codes == ()
        row = _curve_row(preflight)
        assert row.member_key == stale.isoformat() and row.availability is ReferenceAvailability.AVAILABLE
        assert row.reason_codes == ("TREASURY_CURVE_STALE",)
        # The input-ready REFERENCE dependency is built AVAILABLE with the preflight's codes (the R52 failure mode).
        AdvancedOptionsInputDependencyEvidence(
            member_kind="REFERENCE", member_key="GENERATION", status=AdvancedOptionsInputDependencyStatus.AVAILABLE,
            evidence_sha256=canonical_sha256(preflight.preflight_sha256), reason_codes=preflight.reason_codes)
        state = producer.produce()
        assert state.state_sha256 == preflight.state.state_sha256

        # The expected curve arrives: the next generation's row carries no code.
        with cursor(database_pool) as database_cursor:
            _curve(database_cursor, second, expected)
        normal = PostgresAdvancedOptionsReferenceProducer(provider, second, retry_initial_seconds=0).preflight()
        assert normal.status is ReferenceProducerStatus.READY and normal.reason_codes == ()
        assert _curve_row(normal).member_key == expected.isoformat() and _curve_row(normal).reason_codes == ()
