"""R84b (Nov 18): corporate-action drift compares action content, not raw payload hashes (unit level).

Nov 17 06:44: `/api/strategies/daily` answered 503 "Strategy action evidence changed" overnight. After XOM's ex-date
(Nov 16) the provider re-sent the same dividend with two fields added (`historical_adjustment_factor`,
`split_adjusted_cash_amount`); ex-date and cash were unchanged, the payload hash and the coverage response hash were not.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from realtime.contracts import canonical_sha256
from realtime.corporate_action_drift import (
    corporate_action_content,
    corporate_action_coverage_content,
    corporate_action_drift_params,
    corporate_action_drift_sql,
)
from realtime.daily_products import MODE_JOB, daily_products_input_sha256

UTC = timezone.utc
XOM = UUID("00000000-0000-4000-8000-0000000000b1")
T0 = datetime(2026, 11, 15, 23, 47, tzinfo=UTC)
RESENT = datetime(2026, 11, 17, 0, 47, tzinfo=UTC)
XOM_DIVIDEND = {
    "id": "E6436c5475706773f03490acf0b63fdb90b2c72bfeed329a6eb4afc080acd80ae", "ticker": "XOM",
    "cash_amount": 1.07, "currency": "USD", "declaration_date": "2026-10-31", "distribution_type": "recurring",
    "ex_dividend_date": "2026-11-16", "frequency": 4, "pay_date": "2026-12-10", "record_date": "2026-11-16",
}
XOM_RESENT = dict(XOM_DIVIDEND, historical_adjustment_factor=0.993728, split_adjusted_cash_amount=1.07)


def _action(raw, received_at=T0, *, action_type="DIVIDEND", **columns):
    """A loaded action row as the daily-products loader returns it (strategy action details included)."""
    row = {
        "action_id": uuid4(), "security_id": XOM, "ticker": "XOM", "action_type": action_type,
        "effective_date": date.fromisoformat(raw["ex_dividend_date"]), "split_from": None, "split_to": None,
        "payload_sha256": canonical_sha256(raw), "received_at": received_at, "source": "massive",
        "source_key": raw["id"], "observed_at": received_at, "cash_amount": "%.8f" % raw["cash_amount"],
        "ex_date": date.fromisoformat(raw["ex_dividend_date"]), "new_ticker": None, "raw_payload": raw,
        "observation_ties": 1,
    }
    row.update(columns)
    return row


def _coverage(raws, received_at=T0, *, action_type="DIVIDEND", count=None):
    return {
        "coverage_id": uuid4(), "security_id": XOM, "action_type": action_type, "window_start": date(2025, 11, 17),
        "window_end": date(2027, 11, 17), "response_action_count": len(raws) if count is None else count,
        "response_sha256": canonical_sha256(tuple(raws)), "payload_sha256": "c" * 64, "received_at": received_at,
    }


def _inputs(actions, coverage):
    return SimpleNamespace(
        cohort_id=UUID("00000000-0000-4000-8000-0000000000c0"), provider_tier="MASSIVE_ADVANCED",
        session=date(2026, 11, 17), source_manifest_sha256="d" * 64, members=({"ticker": "XOM"},),
        actions=tuple(actions), coverage=tuple(coverage), bars=(), source_lineage="STORED_ACCEPTED",
    )


def test_the_xom_resend_has_the_same_content():
    before, after = _action(XOM_DIVIDEND), _action(XOM_RESENT, RESENT)
    assert before["payload_sha256"] != after["payload_sha256"]  # what R84 compared
    assert corporate_action_content([before]) == corporate_action_content([after])
    first = [_coverage([XOM_DIVIDEND])]
    resent = [_coverage([XOM_RESENT], RESENT), *first]
    assert first[0]["response_sha256"] != resent[0]["response_sha256"]
    assert corporate_action_coverage_content(first, [before]) == corporate_action_coverage_content(resent, [after])
    assert (daily_products_input_sha256(_inputs([before], first), MODE_JOB, 1)
            == daily_products_input_sha256(_inputs([after], resent), MODE_JOB, 1))


def test_a_change_of_what_readers_derive_is_a_change():
    before = corporate_action_content([_action(XOM_DIVIDEND)])
    # The products loader carries no pay date (the SQL predicate compares it: the DB test).
    for field, value in (("cash_amount", 1.08), ("ex_dividend_date", "2026-11-17"), ("currency", "CAD"),
                         ("distribution_type", "special")):
        after = corporate_action_content([_action(dict(XOM_RESENT, **{field: value}), RESENT)])
        assert after != before, field
    split = _action(dict(XOM_DIVIDEND, id="split-1"), action_type="SPLIT", split_from="1.00000000",
                    split_to="2.00000000")
    assert corporate_action_content([split]) != corporate_action_content(
        [dict(split, split_to="3.00000000", received_at=RESENT)])


def test_a_dropped_or_added_action_changes_the_coverage():
    other = dict(XOM_DIVIDEND, id="second", ex_dividend_date="2026-08-14")
    first = [_coverage([XOM_DIVIDEND, other])]
    actions = [_action(XOM_DIVIDEND), _action(other)]
    dropped = [_coverage([XOM_RESENT], RESENT), *first]
    assert (corporate_action_coverage_content(first, actions)
            != corporate_action_coverage_content(dropped, [_action(XOM_RESENT, RESENT), _action(other)]))
    added = [_coverage([XOM_RESENT, other, dict(other, id="third")], RESENT), *first]
    assert corporate_action_coverage_content(first, actions) != corporate_action_coverage_content(added, [
        _action(XOM_RESENT, RESENT), _action(other, RESENT), _action(dict(other, id="third"), RESENT)])


def test_an_unloaded_receipt_falls_back_to_the_response_hash():
    """Without the coverage's actions loaded the response hash stands in (R84's rule), so nothing is hidden."""
    first = [_coverage([XOM_DIVIDEND])]
    resent = [_coverage([XOM_RESENT], RESENT), *first]
    assert corporate_action_coverage_content(first) != corporate_action_coverage_content(resent)
    same = [_coverage([XOM_DIVIDEND], RESENT), *first]
    assert corporate_action_coverage_content(first) == corporate_action_coverage_content(same)
    # Without the payload loaded, the payload hash stands in for its fields (R84's rule).
    bare = {key: value for key, value in _action(XOM_DIVIDEND).items() if key != "raw_payload"}
    assert corporate_action_content([bare]) != corporate_action_content(
        [dict(bare, payload_sha256=canonical_sha256(XOM_RESENT))])


def test_the_sql_predicate_keeps_its_shape_and_compares_content():
    many, one = corporate_action_drift_sql(), corporate_action_drift_sql(single=True)
    assert many.startswith("EXISTS (") and many.count("%s") == 8 and one.count("%s") == 8
    assert "ANY(%s::uuid[])" in many and "ANY(" not in one
    # R84's equalities stay as the first test (so this flags a subset of what R84 flagged), content decides otherwise.
    assert "known_action.payload_sha256=drift_action.payload_sha256 OR jsonb_build_array(" in many
    assert "latest_coverage.response_sha256=drift_coverage.response_sha256 OR (" in many
    for field in ("ex_date", "pay_date", "cash_amount", "split_from", "split_to", "new_ticker", "'currency'",
                  "'distribution_type'", "response_action_count"):
        assert field in many, field
    assert "historical_adjustment_factor" not in many and "split_adjusted_cash_amount" not in many
    params = corporate_action_drift_params(["a"], T0, RESENT)
    assert params == (["a"], T0, RESENT, T0, ["a"], T0, RESENT, T0)
