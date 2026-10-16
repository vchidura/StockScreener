from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

from realtime.equity_portal_projection_worker import (
    enqueue_portal_projection_job_cursor,
)


NOW = datetime(2026, 10, 16, 15, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")


def test_projection_job_requires_exact_cohort_heads(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (2, 1, UUID(int=1)),
        (2, 2, UUID(int=2)),
    ]
    enqueue = MagicMock()
    monkeypatch.setattr(
        "realtime.equity_portal_projection_worker.enqueue_materialization_job_cursor",
        enqueue,
    )

    job_id = enqueue_portal_projection_job_cursor(
        cursor,
        cohort_id=COHORT,
        cutoff=NOW,
        available_at=NOW,
    )

    assert job_id is not None
    job = enqueue.call_args.args[1]
    assert job.interval == "15m"
    assert job.source_revision_id == UUID(int=1)
    assert job.bundle_sha256 is not None


def test_projection_job_is_not_enqueued_for_partial_cohort(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (2, 1, UUID(int=1)),
        (2, 2, None),
    ]
    enqueue = MagicMock()
    monkeypatch.setattr(
        "realtime.equity_portal_projection_worker.enqueue_materialization_job_cursor",
        enqueue,
    )

    assert enqueue_portal_projection_job_cursor(
        cursor,
        cohort_id=COHORT,
        cutoff=NOW,
        available_at=NOW,
    ) is None
    enqueue.assert_not_called()
