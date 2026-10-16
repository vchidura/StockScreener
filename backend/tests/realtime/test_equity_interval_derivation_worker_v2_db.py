from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from realtime.database_access import RealtimeDatabaseRole
from realtime.equity_interval_derivation_worker import (
    enqueue_interval_derivation_jobs_cursor,
    run_interval_derivation_worker_once,
)
from realtime.equity_worker_service import (
    EQUITY_INTERVAL_DERIVE_JOB,
    EquityWorkerConfig,
)
from realtime.stage_database_v2 import V2WorkerDatabase
from tests.realtime.test_equity_recent_repair_v2_db import _provider, _scope
from tests.realtime.test_session_close_coverage_v2_db import _revise_first_bar
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
SESSION = date(2026, 10, 15)


@pytest.fixture
def pool():
    with disposable_v2_database(
        f"test_realtime_v2_derive_{uuid4().hex[:8]}"
    ) as value:
        yield value


def test_closure_jobs_derive_exact_higher_intervals(pool):
    plan = _scope(pool, SESSION)
    provider = _provider(pool)
    instrument_id = plan.identities[0].instrument_id
    checked_at = datetime.now(timezone.utc)
    with provider.transaction(dict_cursor=True) as unit:
        jobs = enqueue_interval_derivation_jobs_cursor(
            unit.cursor,
            cohort_id=plan.cohort_id,
            instrument_ids=(instrument_id,),
            session_date=SESSION,
            checked_at=checked_at,
        )
    assert len(jobs) == 47

    status = run_interval_derivation_worker_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        EquityWorkerConfig(
            cohort_id=plan.cohort_id,
            job_kind=EQUITY_INTERVAL_DERIVE_JOB,
            lease_owner=uuid4(),
            claim_limit=100,
        ),
        checked_at=checked_at,
    )

    assert status.claimed == status.completed == status.output_count == 47
    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT interval,source_kind,count(*) FROM rt.bar_current "
            "WHERE cohort_id=%s AND adjustment_basis='RAW_UNADJUSTED' "
            "AND interval=ANY(%s) GROUP BY interval,source_kind "
            "ORDER BY interval,source_kind",
            (
                str(plan.cohort_id),
                ["15m", "30m", "1h", "1d"],
            ),
        )
        assert unit.cursor.fetchall() == [
            ("15m", "DERIVED", 26),
            ("1d", "DERIVED", 1),
            ("1h", "DERIVED", 7),
            ("30m", "DERIVED", 13),
        ]

    with provider.transaction(read_only=True) as unit:
        unit.cursor.execute(
            "SELECT instrument_id,ticker FROM rt.instruments "
            "JOIN ref.security_revisions USING(security_id) "
            "WHERE instrument_id=%s ORDER BY observed_at DESC LIMIT 1",
            (instrument_id,),
        )
        member = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT MIN(observed_at) FROM rt.bar_current "
            "WHERE cohort_id=%s AND instrument_id=%s AND interval='5m'",
            (str(plan.cohort_id), instrument_id),
        )
        observed_at = unit.cursor.fetchone()[0]
    session = type("Session", (), {
        "bar_starts": (datetime(2026, 10, 15, 13, 30, tzinfo=timezone.utc),),
        "session_date": SESSION,
    })()
    _revise_first_bar(
        pool,
        (plan.cohort_id, (member,), session, observed_at),
        checked_at,
    )
    with provider.transaction(dict_cursor=True) as unit:
        enqueue_interval_derivation_jobs_cursor(
            unit.cursor,
            cohort_id=plan.cohort_id,
            instrument_ids=(instrument_id,),
            session_date=SESSION,
            checked_at=checked_at,
        )
        unit.cursor.execute(
            "SELECT count(*) FROM rt.materialization_jobs "
            "WHERE cohort_id=%s AND job_kind=%s",
            (str(plan.cohort_id), EQUITY_INTERVAL_DERIVE_JOB),
        )
        assert unit.cursor.fetchone()["count"] == 51
    corrected = run_interval_derivation_worker_once(
        V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE),
        EquityWorkerConfig(
            cohort_id=plan.cohort_id,
            job_kind=EQUITY_INTERVAL_DERIVE_JOB,
            lease_owner=uuid4(),
            claim_limit=100,
        ),
        checked_at=checked_at,
    )
    assert corrected.claimed == corrected.completed == corrected.output_count == 4
