from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import inspect
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import advanced_options_lifecycle as lifecycle_module
from realtime.advanced_options_lifecycle import (
    AdvancedOptionsPreparationRefreshRequired,
    AdvancedOptionsPreparationRetryable,
    AdvancedOptionsStageResult,
    advanced_options_preparation_retry_schedule,
    expire_obsolete_advanced_options_preparation_cursor,
    request_advanced_options_preparation_refresh_cursor,
    run_advanced_options_preparation_cycle,
)
from realtime.advanced_options_automatic_preparation import (
    PostgresAutomaticOptionsPreparationExecutor,
)
from realtime.advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
)
from realtime.advanced_options_p8_selection_source import (
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
)
from realtime.provider_budget import ProviderBudgetExceeded
from realtime.provider_budget import (
    MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
    MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
)


UTC = timezone.utc
CHECKED = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
REVISION_ID = UUID("11111111-1111-1111-1111-111111111111")
INTENT_ID = UUID("22222222-2222-2222-2222-222222222222")
OWNER_ID = UUID("33333333-3333-3333-3333-333333333333")
COHORT_ID = UUID("44444444-4444-4444-4444-444444444444")
BINDING_ID = UUID("55555555-5555-5555-5555-555555555555")


def _generalized_capture(*, quote_at):
    expiration = date(2026, 11, 9)
    snapshots = {}
    for underlyer, option_type, kind, strikes in (
        ("QQQ", "C", "call", (600, 601)),
        ("SPY", "P", "put", (670, 671)),
    ):
        for index, strike in enumerate(strikes):
            ticker = (
                f"O:{underlyer}261109{option_type}"
                f"{strike * 1000:08d}"
            )
            snapshots[ticker] = {
                "details": {
                    "ticker": ticker,
                    "contract_type": kind,
                    "expiration_date": expiration.isoformat(),
                    "strike_price": strike,
                    "shares_per_contract": 100,
                },
                "last_quote": {
                    "bid": 1,
                    "ask": 1.1,
                    "last_updated": int(
                        quote_at.timestamp() * 1_000_000_000
                    ),
                },
                "day": {"volume": 100},
                "underlying_asset": {
                    "ticker": underlyer,
                    "price": strike,
                },
            }
            if index:
                snapshots[ticker]["open_interest"] = 100
    return {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
        "configured_underlyings": ["QQQ", "SPY"],
        "captured_at_utc": CHECKED.isoformat(),
        "session_date": CHECKED.date().isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [
            {"underlyer": "QQQ"},
            {"underlyer": "SPY"},
        ],
        "selected_snapshots": snapshots,
    }


def _allocation_executor():
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._exact_four_compatibility = False
    executor._admission_profile_version = (
        ACTIVITY_MOMENTUM_PROFILE_VERSION
    )
    executor._enabled_strategies = ()
    executor._active_quote_contract_budget = None
    executor._per_underlying_contract_limit = None
    executor._measured_capacity_limit = None
    executor._effective_provider_project_ceiling = None
    return executor


def test_capture_fresh_at_review_clock_allocates_after_wall_clock_delay():
    executor = _allocation_executor()
    executor._clock = lambda: CHECKED + timedelta(hours=12)
    capture, allocation = executor._allocate_capture(
        SimpleNamespace(
            revision_id=REVISION_ID,
            session_date=CHECKED.date(),
            option_underlyings=("QQQ", "SPY"),
        ),
        _generalized_capture(
            quote_at=CHECKED - timedelta(seconds=30),
        ),
    )

    assert allocation is not None
    assert capture["activity_momentum_allocation"][
        "allocation_sha256"
    ] == allocation.allocation_sha256
    assert set(capture["selected_snapshots"])


def test_capture_stale_at_review_clock_requests_catalog_refresh():
    executor = _allocation_executor()

    with pytest.raises(
        AdvancedOptionsPreparationRefreshRequired,
        match="CATALOG_QUOTE_CAPTURE_STALE",
    ) as raised:
        executor._allocate_capture(
            SimpleNamespace(
                revision_id=REVISION_ID,
                session_date=CHECKED.date(),
                option_underlyings=("QQQ", "SPY"),
            ),
            _generalized_capture(
                quote_at=CHECKED - timedelta(hours=2),
            ),
        )

    assert raised.value.affected_underlyings == ("QQQ", "SPY")


def test_options_preparation_budget_has_local_and_shared_ceilings():
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._clock = lambda: CHECKED
    executor._request_limit = 4
    executor._byte_limit = 25_000_000
    underlyings = tuple(f"T{index:02d}" for index in range(10))

    budget = executor.provider_budget(SimpleNamespace(
        stage="CATALOG_QUOTE",
        session_date=CHECKED.date(),
        revision_id=REVISION_ID,
        ticker=underlyings[0],
        option_underlyings=underlyings,
    ))

    assert budget is not None
    assert budget.scope.request_ceiling == 120
    assert budget.scope.decoded_byte_ceiling == 750_000_000
    assert budget.scope.effective_account_request_ceiling == (
        MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING
    )
    assert budget.scope.effective_account_decoded_byte_ceiling == (
        MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING
    )


def test_refresh_retry_schedule_waits_for_open_and_caps_backoff():
    class _Calendar:
        def session_open(self, _session_date):
            return CHECKED + timedelta(hours=2)

    preopen_retry, preopen_backoff = (
        advanced_options_preparation_retry_schedule(
            session_date=CHECKED.date(),
            requested_at=CHECKED,
            attempt=1,
            calendar=_Calendar(),
        )
    )
    open_retry, open_backoff = (
        advanced_options_preparation_retry_schedule(
            session_date=CHECKED.date(),
            requested_at=CHECKED + timedelta(hours=3),
            attempt=20,
            calendar=_Calendar(),
        )
    )

    assert preopen_retry == CHECKED + timedelta(hours=2)
    assert preopen_backoff == 30
    assert open_retry == CHECKED + timedelta(
        hours=3,
        seconds=300,
    )
    assert open_backoff == 300


def test_newer_session_supersedes_only_older_unfinished_work():
    class _Calendar:
        def session_close(self, _session_date):
            return CHECKED + timedelta(days=1)

    class _ExpirationCursor:
        def __init__(self):
            self.sql = ""
            self.rowcount = 0
            self.work_updates = []

        def execute(self, sql, parameters=None):
            self.sql = sql
            if "UPDATE rt.advanced_options_preparation_work" in sql:
                self.work_updates.append(parameters)
                self.rowcount = 2
            elif "UPDATE rt.advanced_options_preparation_sessions" in sql:
                self.rowcount = 1
            else:
                self.rowcount = 0

        def fetchall(self):
            return (
                (
                    str(REVISION_ID),
                    date(2026, 11, 2),
                    date(2026, 11, 3),
                ),
                (
                    str(INTENT_ID),
                    date(2026, 11, 3),
                    date(2026, 11, 3),
                ),
            )

    cursor = _ExpirationCursor()
    expired = expire_obsolete_advanced_options_preparation_cursor(
        cursor,
        checked_at=CHECKED,
        calendar=_Calendar(),
    )

    assert expired == ({
        "revision_id": str(REVISION_ID),
        "session_date": "2026-11-02",
        "expiration_state": "SUPERSEDED",
        "expired_work_count": 2,
    },)
    assert len(cursor.work_updates) == 1
    assert cursor.work_updates[0][0] == "SUPERSEDED"


def test_candidate_pool_capacity_estimates_multiple_logical_shards():
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._maximum_contracts_per_logical_shard = 250
    capture = {
        "selected_snapshots": {
            f"O:SPY{i:08d}": {} for i in range(501)
        }
    }
    executor._combined_capture = lambda *_args, **_kwargs: capture
    executor._allocate_capture = lambda _claim, value: (value, None)

    result = executor._capacity(SimpleNamespace(
        ticker="SPY",
        option_underlyings=("SPY",),
    ))

    assert result.watermark == {
        "contract_count": 501,
        "logical_shards": 3,
    }
    assert result.evidence["capture"] == capture


def test_secondary_capacity_stage_reuses_frozen_allocation():
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._load_completed_evidence = lambda *_args, **_kwargs: {
        "watermark": {
            "contract_count": 12,
            "logical_shards": 1,
        },
        "allocation_sha256": "a" * 64,
    }

    result = executor._capacity(SimpleNamespace(
        ticker="SPY",
        option_underlyings=("QQQ", "SPY"),
    ))

    assert result.watermark == {
        "contract_count": 12,
        "logical_shards": 1,
    }
    assert result.evidence == {
        "status": "READY",
        "source_capacity_ticker": "QQQ",
        "allocation_sha256": "a" * 64,
    }


def test_missing_prior_oi_remains_visible_without_blocking_preparation():
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._load_completed_evidence = lambda *_args, **_kwargs: {
        "capture": {
            "semantic_sha256": "a" * 64,
            "selected_snapshots": {
                "O:SPY261106C00600000": {"open_interest": 50},
                "O:SPY261106P00600000": {},
            },
        }
    }

    result = executor._reference_oi(SimpleNamespace(ticker="SPY"))

    assert result.watermark["oi_available_contract_count"] == 1
    assert result.watermark["oi_unavailable_contract_count"] == 1
    assert result.evidence["status"] == "READY_WITH_UNAVAILABLE_CONTEXT"
    assert result.evidence["oi_unavailable_contracts"] == [
        "O:SPY261106P00600000"
    ]


def test_automatic_preparation_has_one_canonical_allocation_decision():
    source = inspect.getsource(PostgresAutomaticOptionsPreparationExecutor)
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)

    assert source.count("self._allocate_capture(") == 1
    assert "_premarket_capture" not in source
    assert "_fresh_detector_allocation" not in source
    assert executor.provider_budget(
        SimpleNamespace(stage="SESSION_ACTIVATION")
    ) is None


def test_lifecycle_active_status_matches_migration_044_contract():
    source = inspect.getsource(
        lifecycle_module.reconcile_advanced_options_lifecycle_cursor
    )

    assert "FINALIZING" not in source
    assert 'status="ACTIVE"' in source
    assert 'current_stage="COMPLETE"' in source


class _Cursor:
    def __init__(self, stage):
        self.stage = stage
        self.sql = ""
        self.rowcount = 0
        self.claimed = False
        self.evidence_sha256 = None
        self.completed = False
        self.failed = False
        self.refresh_requested = False
        self.refresh_frozen = False
        self.retry_at = None
        self.refresh_backoff_seconds = None
        self.budget_waiting = False

    def execute(self, sql, parameters=None):
        self.sql = sql
        self.rowcount = 0
        if "UPDATE rt.advanced_options_preparation_work AS work SET" in sql:
            self.rowcount = 1
        elif "INSERT INTO rt.advanced_options_preparation_evidence" in sql:
            self.evidence_sha256 = parameters[9]
            self.rowcount = 1
        elif (
            "INSERT INTO "
            "rt.advanced_options_preparation_refresh_requests" in sql
        ):
            self.refresh_requested = True
            self.retry_at = parameters[10]
            self.refresh_backoff_seconds = parameters[11]
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
        elif (
            "UPDATE rt.advanced_options_preparation_work SET "
            "status='RETRY'" in sql
        ):
            self.retry_at = parameters[0]
            self.budget_waiting = "failure_streak" in sql
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
            "rt.advanced_options_preparation_evidence_history" in self.sql
        ):
            return (self.evidence_sha256,)
        if (
            "SELECT activated_session_binding_id FROM "
            "rt.advanced_options_preparation_sessions" in self.sql
        ):
            return (BINDING_ID if self.refresh_frozen else None,)
        if (
            "SELECT count(*) FROM "
            "rt.advanced_options_preparation_work" in self.sql
        ):
            return (1 if self.refresh_frozen else 0,)
        if (
            "SELECT 1 FROM rt.advanced_options_preparation_work"
            in self.sql
        ):
            return (1,)
        return None

    def fetchall(self):
        return ()


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
    def __init__(self, *, retry=False, refresh=False):
        self.retry = retry
        self.refresh = refresh
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
        if self.refresh:
            raise AdvancedOptionsPreparationRefreshRequired(
                "CATALOG_QUOTE_CAPTURE_STALE",
                ("SPY",),
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


def test_refresh_transition_survives_worker_restart_before_new_claim():
    first_provider = _Provider("CAPACITY_SHARDS")
    report = run_advanced_options_preparation_cycle(
        first_provider,
        _Executor(refresh=True),
        owner_id=OWNER_ID,
        checked_at=CHECKED,
    )

    assert report["status"] == "REFRESH_REQUIRED"
    assert report["affected_underlyings"] == ["SPY"]
    assert first_provider.cursor.refresh_requested is True
    assert report["retry_at_utc"] == (
        first_provider.cursor.retry_at.isoformat()
    )
    assert first_provider.cursor.refresh_backoff_seconds == 30

    restarted_provider = _Provider("CATALOG_QUOTE")
    retry_at = datetime.fromisoformat(report["retry_at_utc"])
    restarted = run_advanced_options_preparation_cycle(
        restarted_provider,
        _Executor(),
        owner_id=OWNER_ID,
        checked_at=retry_at,
    )

    assert restarted["status"] == "COMPLETE"
    assert restarted["stage"] == "CATALOG_QUOTE"


def test_refresh_transition_refuses_frozen_allocation():
    cursor = _Cursor("CAPACITY_SHARDS")
    cursor.refresh_frozen = True
    claim = SimpleNamespace(
        revision_id=REVISION_ID,
        intent_id=INTENT_ID,
        session_date=CHECKED.date(),
        ticker="SPY",
        stage="CAPACITY_SHARDS",
        attempt=2,
        owner_id=OWNER_ID,
        owner_epoch=3,
        watermark={},
        prior_evidence={},
        option_underlyings=("SPY",),
    )

    refreshed = request_advanced_options_preparation_refresh_cursor(
        cursor,
        claim,
        affected_underlyings=("SPY",),
        requested_at=CHECKED,
        reason_code="CATALOG_QUOTE_CAPTURE_STALE",
    )

    assert refreshed is False
    assert cursor.refresh_requested is False


def test_budget_exhaustion_releases_claim_into_visible_wait(
    monkeypatch,
):
    provider = _Provider("CATALOG_QUOTE")
    executor = _Executor()

    def exhaust_budget(*_args, **_kwargs):
        raise ProviderBudgetExceeded(
            "provider account request ceiling is exhausted"
        )

    monkeypatch.setattr(
        lifecycle_module,
        "_reserve_stage_provider_budget",
        exhaust_budget,
    )
    report = run_advanced_options_preparation_cycle(
        provider,
        executor,
        owner_id=OWNER_ID,
        checked_at=CHECKED,
    )

    assert report["status"] == "WAITING_BUDGET"
    assert report["provider_requests"] == 0
    assert datetime.fromisoformat(report["retry_at_utc"]) > CHECKED
    assert provider.cursor.budget_waiting is True
    assert provider.cursor.failed is False
    assert executor.claims == [("budget", "CATALOG_QUOTE")]


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
