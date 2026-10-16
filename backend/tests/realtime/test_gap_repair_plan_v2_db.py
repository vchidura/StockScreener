from datetime import date, timedelta
from uuid import uuid4

import pytest

from realtime.gap_repair_plan import plan_gap_repair_cursor
from tests.realtime.test_equity_recent_repair_v2_db import _provider
from tests.realtime.test_session_close_coverage_v2_db import _seed_session
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
SESSION = date(2026, 10, 15)


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
        "HISTORICAL_1M_EXECUTOR_NOT_IMPLEMENTED"
    )
    assert by_interval["5m"]["missing_members"] == 0
    assert report["full_session_request_scopes"] == 1
    assert not report["automatic_execution"]
    assert report["provider_requests"] == report["database_writes"] == 0
