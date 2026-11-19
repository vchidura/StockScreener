"""Nov 16: corporate-action drift by content - identical re-checks are not a change (unit level)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from realtime.corporate_action_drift import (
    corporate_action_content,
    corporate_action_coverage_content,
    corporate_action_drift_params,
    corporate_action_drift_sql,
)
from realtime.daily_products import MODE_JOB, daily_products_input_sha256

UTC = timezone.utc
SECURITY = UUID("00000000-0000-4000-8000-0000000000a1")
T0 = datetime(2026, 11, 13, 4, 47, tzinfo=UTC)


def _action(payload="p1", source_key="k1", received_at=T0):
    return {
        "action_id": uuid4(), "security_id": SECURITY, "ticker": "AAPL", "action_type": "DIVIDEND",
        "effective_date": date(2026, 11, 10), "split_from": None, "split_to": None, "payload_sha256": payload,
        "received_at": received_at, "source": "massive", "source_key": source_key, "observed_at": received_at,
        "cash_amount": "0.26", "ex_date": date(2026, 11, 10), "new_ticker": None, "observation_ties": 1,
    }


def _coverage(response="h1", received_at=T0, window_end=date(2027, 11, 12), count=0):
    return {
        "coverage_id": uuid4(), "security_id": SECURITY, "action_type": "SPLIT", "window_start": date(2025, 9, 30),
        "window_end": window_end, "response_action_count": count, "response_sha256": response,
        "payload_sha256": "c" * 64, "received_at": received_at,
    }


def _inputs(actions, coverage):
    return SimpleNamespace(
        cohort_id=UUID("00000000-0000-4000-8000-0000000000c0"), provider_tier="MASSIVE_ADVANCED",
        session=date(2026, 11, 13), source_manifest_sha256="d" * 64, members=({"ticker": "AAPL"},),
        actions=tuple(actions), coverage=tuple(coverage), bars=(), source_lineage="STORED_ACCEPTED",
    )


def test_predicate_shape_and_parameters_line_up():
    many = corporate_action_drift_sql()
    one = corporate_action_drift_sql(single=True)
    assert many.count("%s") == 8 and one.count("%s") == 8
    assert "ANY(%s::uuid[])" in many and "ANY(" not in one
    assert "payload_sha256" in many and "response_sha256" in many
    params = corporate_action_drift_params(["a"], T0, T0 + timedelta(days=1))
    assert params == (["a"], T0, T0 + timedelta(days=1), T0, ["a"], T0, T0 + timedelta(days=1), T0)


def test_action_content_ignores_arrival_of_identical_revisions():
    first = _action()
    repeat = dict(_action(), received_at=T0 + timedelta(days=3))  # new action_id and times, same content
    assert corporate_action_content([first]) == corporate_action_content([repeat])
    assert corporate_action_content([first, repeat]) == corporate_action_content([first])
    assert corporate_action_content([first]) != corporate_action_content([_action(payload="p2")])
    assert corporate_action_content([first]) != corporate_action_content([first, _action(source_key="k2")])


def test_coverage_content_keeps_the_latest_response_only():
    old = _coverage(received_at=T0)
    recheck = _coverage(received_at=T0 + timedelta(days=3), window_end=date(2027, 11, 13))
    assert corporate_action_coverage_content([old]) == corporate_action_coverage_content([recheck, old])
    # R84b: a response with no actions has no content to change; a split in the response (its action not loaded
    # here, so the response hash stands in) is a change.
    changed = _coverage(response="h2", received_at=T0 + timedelta(days=3), count=1)
    assert corporate_action_coverage_content([old]) != corporate_action_coverage_content([changed, old])


def test_daily_products_identity_survives_identical_rechecks():
    before = _inputs([_action()], [_coverage()])
    after = _inputs(
        [dict(_action(), received_at=T0 + timedelta(days=3))],
        [_coverage(received_at=T0 + timedelta(days=3), window_end=date(2027, 11, 13)), _coverage()],
    )
    assert daily_products_input_sha256(before, MODE_JOB, 1) == daily_products_input_sha256(after, MODE_JOB, 1)
    changed = _inputs([_action(payload="p2")], [_coverage()])
    assert daily_products_input_sha256(before, MODE_JOB, 1) != daily_products_input_sha256(changed, MODE_JOB, 1)
    new_split = _inputs([_action()], [_coverage(response="h2", received_at=T0 + timedelta(days=3), count=1),
                                      _coverage()])
    assert daily_products_input_sha256(before, MODE_JOB, 1) != daily_products_input_sha256(new_split, MODE_JOB, 1)
