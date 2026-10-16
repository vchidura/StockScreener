from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from realtime.incremental_health import collect_incremental_health_cursor


def test_health_separates_budget_gaps_backlog_and_served_cutoff():
    cursor = MagicMock()
    now = datetime(2026, 10, 16, 20, tzinfo=timezone.utc)
    release_id = uuid4()
    cursor.fetchone.side_effect = [
        (2, 1, 4_002, 10, 20_010, 1, 1, 0, 0),
        (now, date(2026, 10, 16), now, release_id),
        (now, date(2026, 10, 16), 9, 9, "COMPLETE"),
    ]
    cursor.fetchall.side_effect = [
        [
            ("1m", "NATIVE_REST", "COMPLETE", 8, now, now, now, now),
            ("1m", "NATIVE_REST", "GAP", 1, None, None, now, now),
        ],
        [("EQUITY_INTERVAL_FINALIZE", "PENDING", 1, now, now)],
        [("EQUITY_5M_INPUT_READY", "OPEN", 1, now, now)],
        [("RUNNING", 1, now)],
        [("EQUITY_MATERIALIZE", "RUNNING", 1, now)],
    ]

    report = collect_incremental_health_cursor(
        cursor,
        cohort_id=uuid4(),
        account_alias="MASSIVE_DEVELOPER",
        session_date=date(2026, 10, 16),
        checked_at=now,
    )

    assert report["status"] == "DEGRADED"
    assert set(report["reason_codes"]) == {
        "LEGACY_UNCOORDINATED_PROVIDER_REQUESTS",
        "UNRESOLVED_PROVIDER_REQUESTS",
        "ACQUISITION_GAPS",
        "MATERIALIZATION_BACKLOG",
        "OPEN_OR_FAILED_BARRIERS",
    }
    assert report["provider_budget"]["coordinated_requests"] == 2
    assert report["served_release"]["release_id"] == str(release_id)
    assert report["provider_requests"] == report["database_writes"] == 0
