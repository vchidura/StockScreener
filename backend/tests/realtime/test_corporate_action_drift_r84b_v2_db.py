"""R84b (Nov 18): the XOM re-send against the real schema, written by the production fact builders and writer.

Nov 17 06:44: after XOM's ex-date the provider re-sent the same dividend with `historical_adjustment_factor` and
`split_adjusted_cash_amount` added (00:47 and 05:30 UTC); the strategy read answered 503 "Strategy action evidence
changed". The content predicate must not see that as drift, and must still see every change of what readers derive.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from psycopg2.extras import RealDictCursor, register_uuid

from realtime.corporate_action_drift import (
    corporate_action_content,
    corporate_action_coverage_content,
    corporate_action_drift_params,
    corporate_action_drift_sql,
)
from realtime.reference_adapters import _corporate_action_coverage_fact, _corporate_action_fact, _security_id
from realtime.reference_repository import _persist_reference_fact
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
register_uuid()  # as in production (database_access_v2): the writer verifies stored uuids against its facts
UTC = timezone.utc
T0 = datetime(2026, 11, 15, 23, 47, tzinfo=UTC)        # the nightly re-check before the ex-date
PUBLISHED = datetime(2026, 11, 16, 22, 32, tzinfo=UTC)  # the products published after the Nov 16 close
RESENT = datetime(2026, 11, 17, 0, 47, tzinfo=UTC)      # first re-send with the two fields added
RESENT_AGAIN = datetime(2026, 11, 17, 5, 30, tzinfo=UTC)
READ_AT = datetime(2026, 11, 17, 6, 44, tzinfo=UTC)
START, END = date(2025, 11, 17), date(2027, 11, 17)
XOM_DIVIDEND = {
    "id": "E6436c5475706773f03490acf0b63fdb90b2c72bfeed329a6eb4afc080acd80ae", "ticker": "XOM",
    "cash_amount": 1.07, "currency": "USD", "declaration_date": "2026-10-31", "distribution_type": "recurring",
    "ex_dividend_date": "2026-11-16", "frequency": 4, "pay_date": "2026-12-10", "record_date": "2026-11-16",
}
XOM_RESENT = dict(XOM_DIVIDEND, historical_adjustment_factor=0.993728, split_adjusted_cash_amount=1.07)
EARLIER = dict(XOM_DIVIDEND, id="E-august", ex_dividend_date="2026-08-15", record_date="2026-08-15",
               pay_date="2026-09-10", declaration_date="2026-07-31", cash_amount=1.03)


def _write(cursor, at, dividends, *, actions=True, coverage=True):
    """One coverage refresh as the rollover writes it: every dividend in the window, then both coverage receipts."""
    request = SimpleNamespace(created_at=at)
    facts = [_corporate_action_fact("XOM", "DIVIDEND", row, request) for row in dividends] if actions else []
    if coverage:
        facts += [_corporate_action_coverage_fact("XOM", "DIVIDEND", tuple(dividends), START, END, request),
                  _corporate_action_coverage_fact("XOM", "SPLIT", (), START, END, request)]
    for fact in facts:
        _persist_reference_fact(cursor, fact)


def _drift(cursor, ids, *, single=False):
    cursor.execute("SELECT " + corporate_action_drift_sql(single=single) + " AS action_drift",
                   corporate_action_drift_params(ids, PUBLISHED, READ_AT))
    return bool(cursor.fetchone()["action_drift"])


def _case(cursor, name, write, ids):
    cursor.execute(f"SAVEPOINT {name}")
    try:
        write()
        return _drift(cursor, ids), _drift(cursor, ids[0], single=True)
    finally:
        cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")


def _loaded(cursor, security_id, known_by):
    """The daily-products loader's columns for the newest revision of each action, and the coverage rows."""
    cursor.execute(
        "SELECT DISTINCT ON (source_key) action_id,security_id,ticker,action_type,effective_date,split_from::text,"
        "split_to::text,payload_sha256,received_at,source,source_key,observed_at,cash_amount::text,ex_date,new_ticker,"
        "raw_payload FROM ref.corporate_action_revisions WHERE security_id=%s AND received_at<=%s "
        "ORDER BY source_key,received_at DESC", (security_id, known_by))
    actions = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        "SELECT coverage_id,security_id,action_type,window_start,window_end,response_action_count,response_sha256,"
        "payload_sha256,received_at FROM ref.corporate_action_coverage WHERE security_id=%s AND received_at<=%s",
        (security_id, known_by))
    return actions, [dict(row) for row in cursor.fetchall()]


def test_the_xom_resend_is_not_drift_and_real_changes_are():
    with disposable_v2_database("test_realtime_v2_night_e_r84b_" + uuid4().hex[:8]) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                xom = str(_security_id("XOM"))
                cursor.execute("INSERT INTO ref.securities(security_id,asset_type,source_origin,created_at) "
                               "VALUES(%s,'STOCK','r84b-test',%s)", (xom, T0 - timedelta(days=30)))
                _write(cursor, T0, [EARLIER, XOM_DIVIDEND])
                assert _drift(cursor, [xom]) is False
                before = _loaded(cursor, xom, PUBLISHED)

                # Every reader condition the strategy read evaluated (by itself, then both re-sends together).
                assert _case(cursor, "resent", lambda: _write(cursor, RESENT, [EARLIER, XOM_RESENT]), [xom]) == (
                    False, False)

                def both():
                    _write(cursor, RESENT, [EARLIER, XOM_RESENT])
                    _write(cursor, RESENT_AGAIN, [EARLIER, XOM_RESENT])
                assert _case(cursor, "resent_twice", both, [xom]) == (False, False)

                # The same rows under R84's raw-hash rule: both the revision and the coverage response changed.
                cursor.execute("SAVEPOINT raw")
                both()
                cursor.execute(
                    "SELECT (SELECT count(DISTINCT payload_sha256) FROM ref.corporate_action_revisions "
                    "WHERE security_id=%s AND source_key=%s) AS payloads,"
                    "(SELECT count(DISTINCT response_sha256) FROM ref.corporate_action_coverage "
                    "WHERE security_id=%s AND action_type='DIVIDEND') AS responses",
                    (xom, XOM_DIVIDEND["id"], xom))
                assert dict(cursor.fetchone()) == {"payloads": 2, "responses": 2}
                after = _loaded(cursor, xom, READ_AT)
                cursor.execute("ROLLBACK TO SAVEPOINT raw")
                # The daily products' input identity (actions + latest coverage by content) is the same too.
                assert corporate_action_content(before[0]) == corporate_action_content(after[0])
                assert (corporate_action_coverage_content(before[1], before[0])
                        == corporate_action_coverage_content(after[1], after[0]))

                for name, changed in (
                        ("cash", dict(XOM_RESENT, cash_amount=1.08, split_adjusted_cash_amount=1.08)),
                        ("ex_date", dict(XOM_RESENT, ex_dividend_date="2026-11-17")),
                        ("pay_date", dict(XOM_RESENT, pay_date="2026-12-11")),
                        ("currency", dict(XOM_RESENT, currency="CAD")),
                        ("distribution", dict(XOM_RESENT, distribution_type="special"))):
                    assert _case(cursor, name, lambda changed=changed: _write(cursor, RESENT, [EARLIER, changed]),
                                 [xom]) == (True, True), name
                # A dividend the provider no longer reports, one more, or a receipt whose actions were not written.
                assert _case(cursor, "dropped", lambda: _write(cursor, RESENT, [XOM_RESENT]), [xom]) == (True, True)
                added = dict(XOM_RESENT, id="E-special", distribution_type="special", cash_amount=0.5)
                assert _case(cursor, "added", lambda: _write(cursor, RESENT, [EARLIER, XOM_RESENT, added]),
                             [xom]) == (True, True)
                assert _case(cursor, "receipt_only",
                             lambda: _write(cursor, RESENT, [EARLIER, XOM_RESENT], actions=False), [xom]) == (True, True)
                # Before the publication or after the read: outside the period, as in R84.
                assert _case(cursor, "after_read", lambda: _write(
                    cursor, READ_AT + timedelta(minutes=1), [dict(XOM_RESENT, cash_amount=1.08)]), [xom]) == (
                    False, False)
            connection.rollback()
        finally:
            if not connection.closed:
                connection.rollback()
            pool.putconn(connection)
