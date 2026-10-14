from dataclasses import replace
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import inspect
from uuid import uuid4
from types import SimpleNamespace

import pytest

import realtime.option_pre_session_staging as staging
from realtime.contracts import UniverseManifest
from realtime.option_pre_session_staging import (
    StagedAccountScheduler, StagedOptionPackageClaim, StagedOptionPackageReceipt,
    assemble_staged_collection, claim_staged_package_cursor, fail_staged_package_cursor,
    option_pre_session_run_id, staged_package_set_sha256,
)
from realtime.forward_runtime import DeveloperForwardBudget
from realtime.preflight import ProviderTier
from options.config import load_option_runtime_configuration
from scripts.run_realtime_option_pre_session import build_developer_forward_process_plan
from tests.realtime.test_option_pre_session_producer import _packages, _request, _security_id
from tests.realtime.test_option_session_manifest import MARKET_TIME, _candidates, _developer_policy, _scope


def _staged_underlyings(count=100):
    tickers = tuple(f"X{index:03d}" for index in range(count))
    universe = UniverseManifest("staged-scale-100-v1", tickers, tickers)
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, universe)
    policy = _developer_policy()
    candidates = _candidates(universe, policy.target_dtes, 1)
    packages = _packages(scope, policy, candidates)
    request = _request(scope, policy, maximum_requests=count * 10,
        maximum_bytes=count * 1_000_000)
    run_id = option_pre_session_run_id(request)
    receipts = tuple(StagedOptionPackageReceipt(run_id,
        package.evidence.result.batch.underlyer,
        _security_id(package.evidence.result.batch.underlyer), package,
        provider_requests=2, response_bytes=package.evidence.response_bytes,
        completed_at=MARKET_TIME + timedelta(seconds=index + 1))
        for index, package in enumerate(packages))
    return request, receipts


def test_staged_packages_scale_to_100_underlyings_and_reconstruct_exact_collection():
    request, receipts = _staged_underlyings()

    collection = assemble_staged_collection(request, tuple(reversed(receipts)))

    assert len(collection.packages) == 100
    assert collection.provider_requests == 200
    assert tuple(row.evidence.result.batch.underlyer for row in collection.packages) == request.scope.universe.option_underlyings
    assert staged_package_set_sha256(receipts) == staged_package_set_sha256(tuple(reversed(receipts)))


def test_staged_finalization_refuses_missing_tampered_or_over_budget_receipts():
    request, receipts = _staged_underlyings(12)
    with pytest.raises(ValueError, match="exact universe"):
        assemble_staged_collection(request, receipts[:-1])
    with pytest.raises(ValueError, match="receipt set"):
        assemble_staged_collection(request, (replace(receipts[0], run_id=uuid4()), *receipts[1:]))
    constrained = replace(request, maximum_response_bytes=1)
    rebound = tuple(replace(row, run_id=option_pre_session_run_id(constrained)) for row in receipts)
    with pytest.raises(ValueError, match="response-byte bound"):
        assemble_staged_collection(constrained, rebound)


def test_staged_run_identity_changes_with_scope_budget_or_cycle():
    request, _ = _staged_underlyings(12)
    assert option_pre_session_run_id(request) != option_pre_session_run_id(
        replace(request, maximum_response_bytes=request.maximum_response_bytes + 1))
    assert option_pre_session_run_id(request) != option_pre_session_run_id(
        replace(request, scheduled_cycle=request.scheduled_cycle + timedelta(minutes=1)))


def test_reviewed_staged_process_plan_scales_to_50_option_underlyings():
    stocks = tuple(f"X{index:03d}" for index in range(47))
    etfs = ("SPY", "QQQ", "IWM")
    universe = UniverseManifest("reviewed-forward-50-v1", (*stocks, *etfs), (*stocks, *etfs))
    environment = {"POLYGON_API_KEY": "fixture-only",
        "OPTION_FIXED_STOCK_UNDERLYERS": ",".join(stocks),
        "OPTION_FIXED_ETF_UNDERLYERS": ",".join(etfs),
        "OPTION_STOCK_UNIVERSE_SIZE": str(len(stocks)), "OPTION_ETF_UNIVERSE_SIZE": str(len(etfs)),
        "OPTION_POLICY_FILE": "options/policies/developer_forward_liquidity_v2.json",
        "OPTION_VALUATION_POLICY_FILE": "options/policies/valuation_raw_spot_v2.json",
        "OPTION_STRATEGY_POLICY_FILE": "options/policies/strategy_technical_forward_v2.json",
        "OPTION_RISK_FREE_RATE_SOURCE": "us_treasury_par_yield_v1",
        "OPTION_DIVIDEND_INPUT_SOURCE": "polygon_corporate_actions_v1"}
    configuration = load_option_runtime_configuration(environment,
        __import__("pathlib").Path(__file__).parents[2])

    plan = build_developer_forward_process_plan("stocks_realtime_v1", environ=environment,
        configuration=configuration, maximum_provider_requests=5_000,
        maximum_response_bytes=1_073_741_824, maximum_target_dte_deviation=4,
        target_dtes=(7, 21, 45), universe=universe, staged=True)

    assert len(plan.option_underlyings) == 50
    assert plan.cohort_binding.universe_sha256 == universe.sha256
    assert plan.manifest_policy.maximum_contracts_per_underlying * 50 == 600
    assert plan.maximum_provider_requests == 5_000
    assert plan.maximum_response_bytes == 1_073_741_824


def test_distributed_schedulers_share_pacing_request_and_byte_ledger():
    class UsageCursor:
        def __init__(self):
            self.now = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
            self.requests, self.bytes, self.next_request = 0, 0, None
            self.maximum_requests, self.maximum_bytes = 10, 100
            self.deadline = self.now + timedelta(minutes=5)
            self.scheduled = self.now
            self.result, self.rowcount = [], 1

        def execute(self, sql, params=()):
            if sql == "SELECT clock_timestamp()":
                self.result = [(self.now,)]
            elif "SELECT usage.provider_requests" in sql:
                self.result = [(self.requests, self.next_request, self.maximum_requests,
                    self.deadline, self.scheduled)]
            elif "provider_requests=provider_requests+1" in sql:
                self.requests += 1
                self.next_request = params[0] + timedelta(seconds=params[1])
                self.result = []
            elif "SELECT usage.response_bytes" in sql:
                self.result = [(self.bytes, self.maximum_bytes)]
            elif "SET response_bytes=%s" in sql:
                self.bytes = params[0]
                self.result = []
            elif "SET next_request_at=GREATEST" in sql:
                self.next_request = max(self.next_request or params[0], params[1])
                self.result = []
            else:
                raise AssertionError(sql)

        def fetchone(self):
            return self.result[0] if self.result else None

    class Database:
        def __init__(self, cursor):
            self.cursor = cursor

        @contextmanager
        def transaction(self):
            yield self.cursor

    cursor = UsageCursor()
    waits = []
    def wait(seconds):
        waits.append(seconds)
        cursor.now += timedelta(seconds=seconds)
    budget = DeveloperForwardBudget(maximum_requests=10, maximum_response_bytes=100)
    run_id = uuid4()
    first = StagedAccountScheduler(Database(cursor), run_id, budget, clock=lambda: cursor.now, wait=wait)
    second = StagedAccountScheduler(Database(cursor), run_id, budget, clock=lambda: cursor.now, wait=wait)
    bounds = dict(available_after=cursor.now, deadline=cursor.deadline)

    first.request(lambda: SimpleNamespace(body=b"one"), **bounds)
    second.request(lambda: SimpleNamespace(body=b"two"), **bounds)

    assert cursor.requests == 2 and cursor.bytes == 6
    assert waits == [0.5]
    assert first.requests == second.requests == 1


def test_staged_failure_rejects_an_expired_owner_lease():
    now = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
    owner_id = uuid4()
    claim = StagedOptionPackageClaim(
        uuid4(), "AAPL", owner_id, 2, 1, now - timedelta(seconds=1)
    )

    class ExpiredClaimCursor:
        rowcount = 0

        def execute(self, statement, params=()):
            if statement == "SELECT clock_timestamp()":
                self.result = (now,)
            elif "SELECT work.status" in statement:
                self.result = (
                    "CLAIMED",
                    str(owner_id),
                    2,
                    1,
                    claim.lease_expires_at,
                    now + timedelta(minutes=5),
                )
            else:
                raise AssertionError("expired owner reached the failure update")

        def fetchone(self):
            return self.result

    with pytest.raises(ValueError, match="failure fence changed"):
        fail_staged_package_cursor(
            ExpiredClaimCursor(),
            claim,
            reason="late failure",
            provider_requests=1,
            response_bytes=10,
        )


def test_staged_claim_locks_only_the_mutable_work_table():
    now = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)

    class ClaimCursor:
        def __init__(self):
            self.results = iter(((now,), None))
            self.statements = []

        def execute(self, statement, params=()):
            self.statements.append(statement)

        def fetchone(self):
            return next(self.results)

    cursor = ClaimCursor()
    assert claim_staged_package_cursor(cursor, uuid4(), uuid4()) is None
    assert "FOR UPDATE OF work SKIP LOCKED" in cursor.statements[1]
    source = inspect.getsource(staging)
    assert source.count("FOR UPDATE OF usage") == 2
    assert source.count("FOR UPDATE OF work") == 2