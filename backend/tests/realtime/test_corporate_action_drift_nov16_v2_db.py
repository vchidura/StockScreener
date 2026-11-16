"""Nov 16: the content predicate for corporate-action drift against the real schema."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor

from realtime.corporate_action_drift import corporate_action_drift_params, corporate_action_drift_sql
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
T0 = datetime(2026, 11, 13, 4, 47, tzinfo=UTC)
PUBLISHED = T0 + timedelta(hours=1)
READ_AT = T0 + timedelta(days=5)
P1, P2, H1, H2, H3, C = ("1" * 64, "2" * 64, "a" * 64, "b" * 64, "e" * 64, "c" * 64)


def _security(cursor):
    security_id = uuid4()
    cursor.execute(
        "INSERT INTO ref.securities(security_id,asset_type,source_origin,created_at) VALUES(%s,'STOCK','drift-test',%s)",
        (str(security_id), T0 - timedelta(days=30)))
    return str(security_id)


def _revision(cursor, security_id, *, payload=P1, source_key="k1", at=T0):
    cursor.execute(
        "INSERT INTO ref.corporate_action_revisions(action_id,security_id,ticker,action_type,effective_date,ex_date,"
        "cash_amount,source,source_key,observed_at,received_at,payload_sha256,raw_payload) VALUES"
        "(%s,%s,'AAPL','DIVIDEND',%s,%s,0.26,'massive',%s,%s,%s,%s,%s)",
        (str(uuid4()), security_id, date(2026, 11, 10), date(2026, 11, 10), source_key, at, at, payload,
         Json({"payload": payload})))


def _coverage(cursor, security_id, *, action_type="SPLIT", response=H1, at=T0, window_end=date(2027, 11, 12)):
    cursor.execute(
        "INSERT INTO ref.corporate_action_coverage(coverage_id,security_id,action_type,window_start,window_end,source,"
        "source_key,response_action_count,response_sha256,observed_at,received_at,payload_sha256) VALUES"
        "(%s,%s,%s,%s,%s,'massive',%s,0,%s,%s,%s,%s)",
        (str(uuid4()), security_id, action_type, date(2025, 9, 30), window_end, f"cov:{uuid4()}", response, at, at, C))


def _drift(cursor, ids, *, single=False):
    cursor.execute("SELECT " + corporate_action_drift_sql(single=single) + " AS action_drift",
                   corporate_action_drift_params(ids, PUBLISHED, READ_AT))
    return bool(cursor.fetchone()["action_drift"])


def _case(connection, cursor, name, write, *, ids, single=False):
    cursor.execute(f"SAVEPOINT {name}")
    try:
        write()
        return _drift(cursor, ids, single=single)
    finally:
        cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")


def test_identical_rechecks_are_not_drift_and_real_changes_are():
    with disposable_v2_database("test_realtime_v2_action_drift_" + uuid4().hex[:8]) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                known, fresh = _security(cursor), _security(cursor)
                _revision(cursor, known)
                _coverage(cursor, known)
                _coverage(cursor, known, action_type="DIVIDEND", response=H3)
                later = T0 + timedelta(days=3)
                assert _drift(cursor, [known, fresh]) is False

                def identical():  # the nightly refresh: same content, new rows, moved window
                    _revision(cursor, known, at=later)
                    _coverage(cursor, known, at=later, window_end=date(2027, 11, 13))
                    _coverage(cursor, known, action_type="DIVIDEND", response=H3, at=later)
                assert _case(connection, cursor, "same_many", identical, ids=[known]) is False
                assert _case(connection, cursor, "same_one", identical, ids=known, single=True) is False

                assert _case(connection, cursor, "payload", lambda: _revision(cursor, known, payload=P2, at=later),
                             ids=[known]) is True
                assert _case(connection, cursor, "new_key", lambda: _revision(cursor, known, source_key="k2", at=later),
                             ids=known, single=True) is True
                assert _case(connection, cursor, "response", lambda: _coverage(cursor, known, response=H2, at=later),
                             ids=[known]) is True
                # No coverage known for the security at publication: its first coverage is new evidence.
                assert _case(connection, cursor, "first", lambda: _coverage(cursor, fresh, at=later),
                             ids=[known, fresh]) is True
                # Changes before the publication or after the read are outside the period.
                assert _case(connection, cursor, "before",
                             lambda: _revision(cursor, known, payload=P2, at=PUBLISHED - timedelta(minutes=1)),
                             ids=[known]) is False
                assert _case(connection, cursor, "after",
                             lambda: _revision(cursor, known, payload=P2, at=READ_AT + timedelta(minutes=1)),
                             ids=[known]) is False
            connection.rollback()
        finally:
            if not connection.closed:
                connection.rollback()
            pool.putconn(connection)
