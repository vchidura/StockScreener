from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pytest

from realtime.advanced_options_lifecycle import (
    AdvancedOptionsPreparationRetryable,
    AdvancedOptionsStageResult,
    run_advanced_options_preparation_cycle,
)


UTC = timezone.utc
CHECKED = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
REVISION_ID = UUID("11111111-1111-1111-1111-111111111111")
INTENT_ID = UUID("22222222-2222-2222-2222-222222222222")
OWNER_ID = UUID("33333333-3333-3333-3333-333333333333")
COHORT_ID = UUID("44444444-4444-4444-4444-444444444444")
BINDING_ID = UUID("55555555-5555-5555-5555-555555555555")


class _Cursor:
    def __init__(self, stage):
        self.stage = stage
        self.sql = ""
        self.rowcount = 0
        self.claimed = False
        self.evidence_sha256 = None
        self.completed = False
        self.failed = False

    def execute(self, sql, parameters=None):
        self.sql = sql
        self.rowcount = 0
        if "UPDATE rt.advanced_options_preparation_work AS work SET" in sql:
            self.rowcount = 1
        elif "INSERT INTO rt.advanced_options_preparation_evidence" in sql:
            self.evidence_sha256 = parameters[9]
            self.rowcount = 1
        elif (
            "UPDATE rt.advanced_options_preparation_work SET "
            "status='COMPLETE'" in sql
        ):
            self.completed = True
            self.rowcount = 1
        elif (
            "UPDATE rt.advanced_options_preparation_work SET "
            "status=%s" in sql
        ):
            self.failed = True
            self.rowcount = 1
        elif "UPDATE rt.advanced_options_preparation_sessions SET" in sql:
            self.rowcount = 1

    def fetchone(self):
        if "RETURNING work.revision_id::text" in self.sql:
            if self.claimed:
                return None
            self.claimed = True
            return (
                str(REVISION_ID),
                str(INTENT_ID),
                date(2026, 11, 2),
                "SPY",
                self.stage,
                {
                    "CATALOG_QUOTE": 10,
                    "REFERENCE_OI": 20,
                    "HISTORY": 30,
                    "CAPACITY_SHARDS": 40,
                    "SESSION_ACTIVATION": 50,
                }[self.stage],
                1,
                1,
                CHECKED + timedelta(seconds=60),
                {},
                {},
                ["SPY"],
                "a" * 64,
                str(COHORT_ID),
                str(COHORT_ID),
            )
        if (
            "SELECT evidence_sha256 FROM "
            "rt.advanced_options_preparation_evidence" in self.sql
        ):
            return (self.evidence_sha256,)
        return None


class _Unit:
    def __init__(self, cursor):
        self.cursor = cursor


class _Provider:
    def __init__(self, stage):
        self.cursor = _Cursor(stage)

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield _Unit(self.cursor)


class _Executor:
    def __init__(self, *, retry=False):
        self.retry = retry
        self.claims = []

    def provider_budget(self, claim):
        self.claims.append(("budget", claim.stage))
        return None

    def execute(self, claim, *, provider_request_ids):
        self.claims.append(
            ("execute", claim.stage, provider_request_ids)
        )
        if self.retry:
            raise AdvancedOptionsPreparationRetryable(
                "fixture interruption"
            )
        return AdvancedOptionsStageResult(
            watermark={
                "stage": claim.stage,
                "attempt": claim.attempt,
            },
            evidence={
                "stage": claim.stage,
                "fixture": "provider-blocked",
            },
            session_binding_id=(
                BINDING_ID
                if claim.stage == "SESSION_ACTIVATION"
                else None
            ),
        )


@pytest.mark.parametrize(
    "stage",
    (
        "CATALOG_QUOTE",
        "REFERENCE_OI",
        "HISTORY",
        "CAPACITY_SHARDS",
        "SESSION_ACTIVATION",
    ),
)
def test_stage_worker_claims_executes_and_commits_immutable_evidence(stage):
    provider = _Provider(stage)
    executor = _Executor()

    report = run_advanced_options_preparation_cycle(
        provider,
        executor,
        owner_id=OWNER_ID,
        checked_at=CHECKED,
    )

    assert report["status"] == "COMPLETE"
    assert report["stage"] == stage
    assert provider.cursor.completed is True
    assert provider.cursor.failed is False
    assert provider.cursor.evidence_sha256 is not None
    assert executor.claims == [
        ("budget", stage),
        ("execute", stage, ()),
    ]
    if stage == "SESSION_ACTIVATION":
        assert report["session_binding_id"] == str(BINDING_ID)


def test_stage_worker_records_retryable_interruption_without_success():
    provider = _Provider("HISTORY")
    executor = _Executor(retry=True)

    report = run_advanced_options_preparation_cycle(
        provider,
        executor,
        owner_id=OWNER_ID,
        checked_at=CHECKED,
    )

    assert report["status"] == "RETRY"
    assert provider.cursor.failed is True
    assert provider.cursor.completed is False
