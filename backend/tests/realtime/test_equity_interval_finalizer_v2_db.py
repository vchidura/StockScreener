"""Disposable-DB coverage for the provider-free 1m-to-5m finalizer."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import exchange_calendars
import pandas as pd
import pytest
from psycopg2.extras import RealDictCursor

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from realtime.database_access import RealtimeDatabaseRole
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.equity_interval_finalizer import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
    enqueue_five_minute_finalizer_jobs_cursor,
    run_five_minute_finalizer_once,
)
from realtime.materialization_jobs import evaluate_job_barrier_cursor
from realtime.worker_database import RealtimeProcessDatabase
from v2_database import (
    disposable_v2_database,
    insert_cohort,
    insert_service_plan,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
DATABASE = "test_realtime_v2_finalizer"


class Database(RealtimeProcessDatabase):
    def __init__(self, pool):
        self._pool = pool
        self._process_plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)

    @contextmanager
    def transaction(
        self,
        *,
        read_only: bool | None = None,
        dict_cursor: bool = False,
        isolation_level: str | None = None,
    ):
        connection = self._pool.getconn()
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            with connection.cursor(cursor_factory=cursor_factory) as cursor:
                if read_only:
                    cursor.execute("SET TRANSACTION READ ONLY")
                yield cursor
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)


@pytest.fixture(scope="module")
def database():
    with disposable_v2_database(DATABASE) as pool:
        yield Database(pool)


def _closed_session_window() -> tuple[datetime, datetime]:
    now = datetime.now(UTC)
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(pd.Timestamp(now.date()), direction="previous")
    close = calendar.session_close(session).to_pydatetime().astimezone(UTC)
    if close >= now:
        session = calendar.previous_session(session)
    start = calendar.session_open(session).to_pydatetime().astimezone(UTC)
    return start, start + timedelta(minutes=5)


def _seed_member(cursor, ticker: str, effective_at: datetime) -> tuple[object, int]:
    security_id = uuid4()
    revision_id = uuid4()
    cursor.execute(
        "INSERT INTO ref.securities "
        "(security_id, asset_type, source_origin, created_at) "
        "VALUES (%s, 'STOCK', 'fixture', %s)",
        (str(security_id), effective_at),
    )
    cursor.execute(
        "INSERT INTO ref.security_revisions "
        "(revision_id, security_id, ticker, exchange, calendar_id, "
        "effective_from, observed_at, payload_sha256, company_name, "
        "security_type, active) "
        "VALUES (%s, %s, %s, 'XNAS', 'XNYS', %s, %s, %s, %s, 'CS', true)",
        (
            str(revision_id),
            str(security_id),
            ticker,
            effective_at,
            effective_at,
            uuid4().hex + uuid4().hex,
            ticker,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.instruments (asset_class, security_id) "
        "VALUES ('EQUITY', %s) RETURNING instrument_id",
        (str(security_id),),
    )
    return security_id, int(cursor.fetchone()[0])


def _minute_fact(cohort_id, security_id, instrument_id, ticker, start, index):
    end = start + timedelta(minutes=1)
    revision = EquityBarRevision(
        bar_revision_id=uuid4(),
        security_id=security_id,
        ticker=ticker,
        interval="1m",
        session_date=start.date(),
        bar_start=start,
        bar_end=end,
        open_price=Decimal(100 + index),
        high_price=Decimal(102 + index),
        low_price=Decimal(99 + index),
        close_price=Decimal(101 + index),
        volume=Decimal("1000"),
        vwap=Decimal("100.5"),
        transaction_count=10,
        source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=True,
        system_observed_at=end + timedelta(seconds=5),
        replay_available_at=None,
        adjusted=False,
        payload_sha256=uuid4().hex + uuid4().hex,
        ingestion_segment_id=uuid4(),
        session_scope=BarSessionScope.RTH,
    )
    return adapt_developer_equity_bar(
        revision, cohort_id=cohort_id, instrument_id=instrument_id
    )


def test_complete_and_gap_jobs_persist_atomic_bar_job_and_barrier_outcomes(database):
    start, end = _closed_session_window()
    deadline = end + timedelta(seconds=30)
    barrier_id = uuid4()
    owner = uuid4()

    with database.transaction() as cursor:
        cohort_id = insert_cohort(cursor, start - timedelta(hours=1))
        plan_id = insert_service_plan(
            cursor,
            cohort_id,
            ("REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE"),
            start - timedelta(hours=1),
        )
        first_security, first_instrument = _seed_member(
            cursor, "AAPL", start - timedelta(days=1)
        )
        second_security, second_instrument = _seed_member(
            cursor, "MSFT", start - timedelta(days=1)
        )
        window_id = uuid4()
        cursor.execute(
            "INSERT INTO rt.window_runs "
            "(window_id, plan_id, cohort_id, provider_tier, window_key, "
            "release_at, cutoff, deadline) "
            "VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', %s, %s, %s, %s)",
            (
                str(window_id),
                str(plan_id),
                str(cohort_id),
                f"finalizer:{window_id}",
                end,
                end,
                deadline,
            ),
        )
        facts = tuple(
            _minute_fact(
                cohort_id, first_security, first_instrument, "AAPL",
                start + timedelta(minutes=index), index,
            )
            for index in range(5)
        ) + tuple(
            _minute_fact(
                cohort_id, second_security, second_instrument, "MSFT",
                start + timedelta(minutes=index), index,
            )
            for index in range(4)
        )
        persist_realtime_equity_bars_cursor(cursor, facts)
        enqueued = enqueue_five_minute_finalizer_jobs_cursor(
            cursor,
            cohort_id=cohort_id,
            window_id=window_id,
            instrument_ids=(first_instrument, second_instrument),
            checked_at=deadline,
            policy_sha256="a" * 64,
            build_id="db-test-1",
            barrier_id=barrier_id,
        )
        assert [row.input.outcome.value for row in enqueued] == ["COMPLETE", "GAP"]

    status = run_five_minute_finalizer_once(
        database,
        EquityWorkerConfig(
            cohort_id=cohort_id,
            job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
            lease_owner=owner,
            claim_limit=2,
        ),
        checked_at=deadline,
    )

    assert (status.claimed, status.completed, status.output_count) == (2, 2, 1)
    with database.transaction() as cursor:
        cursor.execute(
            "SELECT instrument_id, terminal_state, reason_codes "
            "FROM rt.acquisition_watermarks "
            "WHERE cohort_id=%s AND interval='5m' AND source_kind='DERIVED' "
            "ORDER BY instrument_id",
            (str(cohort_id),),
        )
        assert cursor.fetchall() == [
            (first_instrument, "COMPLETE", []),
            (second_instrument, "GAP", ["FINALIZER_GAP"]),
        ]
        cursor.execute(
            "SELECT instrument_id, status FROM rt.materialization_jobs "
            "WHERE cohort_id=%s AND job_kind=%s ORDER BY instrument_id",
            (str(cohort_id), EQUITY_INTERVAL_FINALIZE_JOB),
        )
        assert cursor.fetchall() == [
            (first_instrument, "COMPLETE"),
            (second_instrument, "COMPLETE"),
        ]
        cursor.execute(
            "SELECT instrument_id, count(*) FROM rt.bar_current "
            "WHERE cohort_id=%s AND interval='5m' "
            "GROUP BY instrument_id ORDER BY instrument_id",
            (str(cohort_id),),
        )
        assert cursor.fetchall() == [(first_instrument, 1)]
        verdict = evaluate_job_barrier_cursor(cursor, barrier_id)
        assert verdict.state == "FAILED"
        assert verdict.complete_members == (f"instrument:{first_instrument}:5m",)
        assert verdict.failed_members == (f"instrument:{second_instrument}:5m",)
