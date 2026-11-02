from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

from realtime.portal_publisher import PortalPublicationRetryable
from scripts import run_realtime_v2_portal_publication_retry as script


UTC = timezone.utc
CHECKED = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)
COHORT_ID = UUID("11111111-1111-1111-1111-111111111111")
RECEIPTS = tuple(
    UUID(f"22222222-2222-2222-2222-{index:012d}")
    for index in range(1, 5)
)


class _Cursor:
    def __init__(self):
        self.sql = ""

    def execute(self, sql, _parameters=None):
        self.sql = sql

    def fetchall(self):
        return [
            (
                "STOCK_ALERTS",
                str(receipt),
                str(COHORT_ID),
                "MASSIVE_ADVANCED",
                "INTRADAY",
            )
            for receipt in RECEIPTS
        ]


class _Unit:
    def __init__(self, cursor):
        self.cursor = cursor


class _Provider:
    def __init__(self):
        self.cursor = _Cursor()

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield _Unit(self.cursor)


def test_publication_retry_cycle_handles_pending_retry_expired_and_terminal(
    monkeypatch,
):
    provider = _Provider()
    calls = []

    def resume(_database, _config, **values):
        receipt = values["source_receipt_id"]
        calls.append(receipt)
        if receipt == RECEIPTS[1]:
            raise PortalPublicationRetryable(
                receipt,
                CHECKED + timedelta(seconds=30),
                "RETRY_PENDING",
            )
        if receipt == RECEIPTS[3]:
            raise ValueError("terminal publication work")
        return SimpleNamespace(release=SimpleNamespace(release_id=receipt))

    monkeypatch.setattr(
        script,
        "resume_portal_publication_once",
        resume,
    )

    report = script.run_publication_retry_cycle(
        provider,
        object(),
        checked_at=CHECKED,
        claim_limit=8,
    )

    assert report["status"] == "DEGRADED"
    assert report["due"] == 4
    assert report["completed"] == 2
    assert report["deferred"] == 1
    assert len(report["terminal"]) == 1
    assert calls == list(RECEIPTS)
    assert "state IN ('PENDING','RETRY')" in provider.cursor.sql
    assert "state='CLAIMED' AND lease_until<=%s" in provider.cursor.sql


def test_publication_retry_restart_reuses_completed_work_without_recompute(
    monkeypatch,
):
    provider = _Provider()
    retained = SimpleNamespace(
        release=SimpleNamespace(release_id=RECEIPTS[0])
    )
    results = []

    def resume(*_args, **values):
        results.append(values["source_receipt_id"])
        return retained

    monkeypatch.setattr(
        script,
        "resume_portal_publication_once",
        resume,
    )

    first = script.run_publication_retry_cycle(
        provider,
        object(),
        checked_at=CHECKED,
        claim_limit=8,
    )
    second = script.run_publication_retry_cycle(
        provider,
        object(),
        checked_at=CHECKED + timedelta(seconds=1),
        claim_limit=8,
    )

    assert first["completed"] == second["completed"] == 4
    assert first["provider_requests"] == second["provider_requests"] == 0
    assert results == [*RECEIPTS, *RECEIPTS]
