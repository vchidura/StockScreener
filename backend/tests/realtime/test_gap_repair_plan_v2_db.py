from datetime import date, timedelta
from uuid import uuid4

import pytest
from psycopg2.extras import execute_values

from realtime.gap_repair_plan import plan_gap_repair_cursor
from realtime.session_close_coverage import five_minute_session
from tests.realtime.test_equity_recent_repair_v2_db import _provider
from tests.realtime.test_session_close_coverage_v2_db import _seed_session
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
SESSION = date(2026, 10, 15)
SECOND_SESSION = date(2026, 10, 16)


@pytest.fixture
def pool():
    with disposable_v2_database(
        f"test_realtime_v2_gap_plan_{uuid4().hex[:8]}"
    ) as value:
        yield value


def test_gap_plan_distinguishes_missing_1m_from_complete_5m(pool):
    cohort, _, session, observed_at = _seed_session(
        pool,
        SESSION,
        ("AAPL",),
    )
    provider = _provider(pool)
    with provider.transaction(read_only=True) as unit:
        report = plan_gap_repair_cursor(
            unit.cursor,
            cohort_id=cohort,
            sessions=(SESSION,),
            known_by=observed_at + timedelta(minutes=1),
        )

    by_interval = {row["interval"]: row for row in report["plans"]}
    assert by_interval["1m"]["missing_members"] == 390
    assert by_interval["1m"]["executor"] == (
        "ADVANCED_HISTORICAL_1M"
    )
    assert by_interval["5m"]["executor"] == "LOCAL_DERIVATION_FROM_RAW_1M"
    assert by_interval["5m"]["missing_members"] == 0
    assert report["full_session_request_scopes"] == 1
    assert not report["automatic_execution"]
    assert report["provider_requests"] == report["database_writes"] == 0


def _insert_complete_bars(
    pool,
    *,
    cohort,
    members,
    session_date,
    interval,
    tickers,
):
    session = five_minute_session(session_date)
    width = timedelta(minutes=1 if interval == "1m" else 5)
    starts = []
    current = session.opened_at
    while current < session.closed_at:
        starts.append(current)
        current += width
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rt.ensure_partitions('rt.bar_revisions',%s)",
                (session_date,),
            )
            cursor.execute(
                "SELECT instrument.instrument_id,instrument.security_id "
                "FROM rt.instruments AS instrument "
                "WHERE instrument.instrument_id=ANY(%s::bigint[])",
                ([instrument for instrument, _ticker in members],),
            )
            securities = {
                int(instrument): security
                for instrument, security in cursor.fetchall()
            }
            catalog = []
            revisions = []
            current_rows = []
            observed_at = session.closed_at + timedelta(minutes=20)
            for instrument, ticker in members:
                if ticker not in tickers:
                    continue
                for bar_start in starts:
                    revision_id = uuid4()
                    bar_end = bar_start + width
                    segment_id = uuid4()
                    catalog.append((
                        str(revision_id),
                        str(cohort),
                        "MASSIVE_DEVELOPER",
                        instrument,
                        interval,
                        bar_start,
                        bar_end,
                        observed_at,
                        session_date,
                    ))
                    revisions.append((
                        str(revision_id),
                        str(cohort),
                        "MASSIVE_DEVELOPER",
                        str(securities[instrument]),
                        instrument,
                        ticker,
                        interval,
                        session_date,
                        "RTH",
                        "RAW_UNADJUSTED",
                        bar_start,
                        bar_end,
                        100,
                        101,
                        99,
                        100,
                        1000,
                        "NATIVE_REST",
                        "LIVE_OBSERVED",
                        True,
                        observed_at,
                        str(segment_id),
                        uuid4().hex + uuid4().hex,
                    ))
                    current_rows.append((
                        str(cohort),
                        "MASSIVE_DEVELOPER",
                        instrument,
                        interval,
                        bar_start,
                        "RTH",
                        "RAW_UNADJUSTED",
                        "NATIVE_REST",
                        str(revision_id),
                        bar_end,
                        observed_at,
                        observed_at,
                    ))
            execute_values(
                cursor,
                "INSERT INTO rt.bar_catalog ("
                "bar_revision_id,cohort_id,provider_tier,instrument_id,"
                "interval,bar_start,bar_end,observed_at,session_date"
                ") VALUES %s",
                catalog,
            )
            execute_values(
                cursor,
                "INSERT INTO rt.bar_revisions ("
                "bar_revision_id,cohort_id,provider_tier,security_id,"
                "instrument_id,ticker,interval,session_date,session_scope,"
                "adjustment_basis,bar_start,bar_end,open_price,high_price,"
                "low_price,close_price,volume,source_kind,availability_mode,"
                "is_final,observed_at,ingestion_segment_id,payload_sha256"
                ") VALUES %s",
                revisions,
            )
            execute_values(
                cursor,
                "INSERT INTO rt.bar_current ("
                "cohort_id,provider_tier,instrument_id,interval,bar_start,"
                "session_scope,adjustment_basis,source_kind,bar_revision_id,"
                "bar_end,observed_at,updated_at) VALUES %s",
                current_rows,
            )
        connection.commit()
    finally:
        pool.putconn(connection)


def test_target9_plans_only_eight_missing_members_for_one_and_two_sessions(
    pool,
):
    tickers = ("SPY", "AAPL", "AMZN", "IWM", "MSFT", "NFLX", "NVDA", "QQQ", "TSLA")
    cohort, members, _session, observed_at = _seed_session(
        pool,
        SESSION,
        tickers,
    )
    _insert_complete_bars(
        pool,
        cohort=cohort,
        members=members,
        session_date=SESSION,
        interval="1m",
        tickers={"SPY"},
    )
    _insert_complete_bars(
        pool,
        cohort=cohort,
        members=members,
        session_date=SECOND_SESSION,
        interval="1m",
        tickers={"SPY"},
    )
    _insert_complete_bars(
        pool,
        cohort=cohort,
        members=members,
        session_date=SECOND_SESSION,
        interval="5m",
        tickers=set(tickers),
    )
    provider = _provider(pool)
    with provider.transaction(read_only=True) as unit:
        one = plan_gap_repair_cursor(
            unit.cursor,
            cohort_id=cohort,
            sessions=(SESSION,),
            known_by=observed_at + timedelta(days=2),
        )
        two = plan_gap_repair_cursor(
            unit.cursor,
            cohort_id=cohort,
            sessions=(SESSION, SECOND_SESSION),
            known_by=observed_at + timedelta(days=2),
        )

    one_minute = [
        row for row in one["plans"] if row["interval"] == "1m"
    ]
    two_minute = [
        row for row in two["plans"] if row["interval"] == "1m"
    ]
    assert one["full_session_request_scopes"] == 8
    assert two["full_session_request_scopes"] == 16
    assert sum(row["missing_range_count"] for row in one_minute) == 8
    assert sum(row["missing_range_count"] for row in two_minute) == 16
    assert all(
        missing["ticker"] != "SPY"
        for plan in two_minute
        for missing in plan["missing_ranges"]
    )
    assert all(
        row["missing_members"] == 0
        for row in two["plans"]
        if row["interval"] == "5m"
    )
