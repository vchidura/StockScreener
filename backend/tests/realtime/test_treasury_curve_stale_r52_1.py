"""R52.1 (Nov 18): a stale Treasury curve (R52) is stored in the reference evidence with the ADVISORY code
TREASURY_CURVE_STALE, and nothing blocks on it.

Before: the row was AVAILABLE with no code (only the log line and the DEGRADED fault said so). A plain reason code
would have stopped every window of a stale-curve day (R52's failure): the preflight's `reason_codes` feeds the
input-ready REFERENCE dependency, which refuses an AVAILABLE dependency with reasons, and the opening loader's `missing`
retries preparation on any code. Advisory codes are stored, and neither collector nor the READY decision counts them.
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from realtime import advanced_options_reference_producer as producer
from realtime.advanced_options_input import (
    AdvancedOptionsInputDependencyEvidence,
    AdvancedOptionsInputDependencyStatus,
)
from realtime.advanced_options_reference_state import ReferenceAvailability

UTC = timezone.utc


class _Cursor:
    """The reference producer's curve query: the latest stored curve at or before the session."""

    def __init__(self, rate_date):
        self.rate_date = rate_date

    def execute(self, *_args):
        pass

    def fetchall(self):
        if self.rate_date is None:
            return []
        observed = datetime.combine(self.rate_date, datetime.min.time(), UTC) + timedelta(hours=22)
        return [dict(observation_id="00000000-0000-0000-0000-%012d" % tenor, rate_date=self.rate_date, tenor_days=tenor,
                     annual_rate=0.04, source="us_treasury_par_yield_v1",
                     source_key=f"{self.rate_date.isoformat()}:{tenor}d", source_observed_at=observed,
                     received_at=observed + timedelta(minutes=5), payload_sha256="0" * 64)
                for tenor in (30, 91, 365)]


def _plan(session):
    known = datetime.combine(session, datetime.min.time(), UTC) + timedelta(hours=14, minutes=35)
    return SimpleNamespace(control_plane=SimpleNamespace(session_date=session), known_at=known,
                           valid_until=known + timedelta(hours=7), plan_id=uuid4(), plan_sha256="1" * 64)


def _curve_row(stored, session):
    evidence = []
    producer._load_treasury_curve(_Cursor(stored), _plan(session), evidence)
    (row,) = evidence
    return row


def _state():
    return SimpleNamespace(state_id=uuid4(), state_sha256="2" * 64)


def test_a_stale_curve_is_stored_with_its_advisory_code():
    row = _curve_row(date(2026, 11, 9), date(2026, 11, 12))  # Nov 10's curve missing: Nov 9's is used
    assert row.member_kind == "TREASURY_CURVE" and row.member_key == "2026-11-09"
    assert row.availability is ReferenceAvailability.AVAILABLE
    assert row.reason_codes == ("TREASURY_CURVE_STALE",)


def test_the_expected_curve_row_is_unchanged():
    """A normal day (and the day after a Treasury holiday) stores the row exactly as before R52.1."""
    for stored, session in ((date(2026, 11, 10), date(2026, 11, 12)), (date(2026, 11, 16), date(2026, 11, 17))):
        row = _curve_row(stored, session)
        plan = _plan(session)
        curve, _ = producer._load_treasury_curve(_Cursor(stored), plan, [])
        before = producer._evidence("TREASURY_CURVE", stored.isoformat(), ReferenceAvailability.AVAILABLE, curve,
                                    observed_at=max(item.source_observed_at for item in curve),
                                    known_at=plan.known_at, valid_until=plan.valid_until)
        assert row == before and row.reason_codes == ()


def test_a_stale_day_stays_ready_and_blocks_no_collector():
    session = date(2026, 11, 12)
    stale, normal = _curve_row(date(2026, 11, 9), session), _curve_row(date(2026, 11, 10), session)
    plan, state = _plan(session), _state()
    stale_result = producer._result(plan, [stale], state)
    normal_result = producer._result(plan, [normal], state)
    assert stale_result.status is producer.ReferenceProducerStatus.READY
    assert stale_result.reason_codes == () == normal_result.reason_codes
    assert stale_result.evidence[0].reason_codes == ("TREASURY_CURVE_STALE",)
    # The reference identity changes only because the code (and the older curve) is in the evidence.
    assert stale_result.preflight_sha256 != normal_result.preflight_sha256
    same_curve_without_code = producer.AdvancedOptionsReferenceProducerEvidence(
        member_kind=stale.member_kind, member_key=stale.member_key, availability=stale.availability,
        evidence_sha256=stale.evidence_sha256, observed_at=stale.observed_at, known_at=stale.known_at,
        valid_until=stale.valid_until)
    assert producer._result(plan, [same_curve_without_code], state).preflight_sha256 != stale_result.preflight_sha256
    # The input-ready stage builds its REFERENCE dependency from these: AVAILABLE with the preflight's codes. With a
    # plain (blocking) code this raised 'available Advanced Options input dependency has reasons' in every window.
    dependency = AdvancedOptionsInputDependencyEvidence(
        member_kind="REFERENCE", member_key="GENERATION", status=AdvancedOptionsInputDependencyStatus.AVAILABLE,
        evidence_sha256="3" * 64, reason_codes=stale_result.reason_codes)
    assert dependency.reason_codes == ()
    # Next to a blocking code the advisory one is not reported as a blocker either.
    blocked = producer._result(plan, [stale, _curve_row(None, session)], state)
    assert blocked.status is producer.ReferenceProducerStatus.BLOCKED
    assert blocked.reason_codes == ("TREASURY_CURVE_MISSING",)


def test_a_missing_curve_still_blocks():
    session = date(2026, 11, 12)
    row = _curve_row(None, session)
    assert row.availability is ReferenceAvailability.UNAVAILABLE and row.reason_codes == ("TREASURY_CURVE_MISSING",)
    result = producer._result(_plan(session), [row], _state())
    assert result.status is producer.ReferenceProducerStatus.BLOCKED
    assert result.reason_codes == ("TREASURY_CURVE_MISSING",)


def test_a_curve_too_old_is_still_refused():
    with pytest.raises(ValueError, match="Treasury curve identity, freshness, or clocks changed"):
        producer._load_treasury_curve(_Cursor(date(2026, 11, 2)), _plan(date(2026, 11, 12)), [])


def test_only_advisory_codes_may_ride_on_an_available_row():
    common = dict(member_kind="TREASURY_CURVE", member_key="2026-11-09", evidence_sha256="4" * 64)
    producer.AdvancedOptionsReferenceProducerEvidence(
        availability=ReferenceAvailability.AVAILABLE, reason_codes=("TREASURY_CURVE_STALE",), **common)
    with pytest.raises(ValueError, match="availability is invalid"):
        producer.AdvancedOptionsReferenceProducerEvidence(
            availability=ReferenceAvailability.AVAILABLE, reason_codes=("TREASURY_CURVE_MISSING",), **common)
    with pytest.raises(ValueError, match="availability is invalid"):  # an UNAVAILABLE row needs a blocking code
        producer.AdvancedOptionsReferenceProducerEvidence(
            availability=ReferenceAvailability.UNAVAILABLE, reason_codes=("TREASURY_CURVE_STALE",), **common)
    producer.AdvancedOptionsReferenceProducerEvidence(
        availability=ReferenceAvailability.UNAVAILABLE,
        reason_codes=("TREASURY_CURVE_STALE", "TREASURY_CURVE_MISSING"), **common)
    assert producer.ADVISORY_REASON_CODES == frozenset({"TREASURY_CURVE_STALE"})


def test_the_opening_loader_does_not_retry_on_a_stale_curve(monkeypatch):
    """Session activation (`load_opening_reference_inputs_cursor`) retries preparation on any code in `missing`."""
    monkeypatch.setattr(producer, "_load_bars_and_metrics", lambda *_args: (("hv",), ("metric",), False))
    monkeypatch.setattr(producer, "_load_valuations", lambda *_args: (("valuation",), False))
    session = date(2026, 11, 12)
    known = datetime.combine(session, datetime.min.time(), UTC) + timedelta(hours=13)

    def load(stored):
        return producer.load_opening_reference_inputs_cursor(
            _Cursor(stored), session_date=session, known_at=known, valid_until=known + timedelta(hours=8),
            underlyings=("SPY",), contracts=(), spots={}, history_cohort_id=uuid4(),
            model_bundle_sha256="5" * 64, valuation_policy_sha256="6" * 64)

    assert load(date(2026, 11, 9)) == (("hv",), ("valuation",), ("metric",), ())
    assert load(date(2026, 11, 10)) == (("hv",), ("valuation",), ("metric",), ())
    assert load(None)[3] == ("TREASURY_CURVE_MISSING",)
