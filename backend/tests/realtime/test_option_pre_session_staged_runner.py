from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.option_manifest_repository import option_manifest_selection_id
from realtime.option_pre_session_staged_runner import StagedTargetOptionPreSessionRunner
from realtime.option_pre_session_staging import (
    StagedOptionPackageClaim, option_pre_session_run_id,
)
from tests.realtime.test_option_pre_session_producer import _packages
from tests.realtime.test_option_pre_session_target import (
    _collection, _request_for, _target_plan, _wired_database,
)
from tests.realtime.test_option_session_manifest import MARKET_TIME
from realtime.database_access import RealtimeDatabaseRole


class StagedSource:
    def __init__(self, packages, *, fail=None):
        self.packages = {row.evidence.result.batch.underlyer: row for row in packages}
        self.fail = fail
        self.requests = 0
        self.response_bytes = 0

    def account_counters(self):
        return self.requests, self.response_bytes

    def collect_underlyer(self, request, underlyer):
        package = self.packages[underlyer]
        self.requests += 2
        self.response_bytes += package.evidence.response_bytes
        if underlyer == self.fail:
            raise ValueError("synthetic package failure")
        return package, 2, package.evidence.response_bytes


def _runner(source, **overrides):
    references, _, _ = _wired_database(RealtimeDatabaseRole.REFERENCES)
    control, _, _ = _wired_database(RealtimeDatabaseRole.CONTROL)
    plan = _target_plan()
    request = _request_for(plan)
    defaults = dict(loader=lambda cursor, active: request,
        run_repository=lambda cursor, active, **kwargs: option_pre_session_run_id(active),
        package_repository=lambda cursor, package: None,
        clock=lambda: MARKET_TIME + timedelta(seconds=30))
    defaults.update(overrides)
    return StagedTargetOptionPreSessionRunner(references, control, source, **defaults), plan, request


def test_staged_runner_persists_each_ticker_independently_then_finalizes_once():
    plan = _target_plan()
    collection = _collection(plan)
    source = StagedSource(collection.packages)
    receipts = []
    underlyings = iter(plan.scope.universe.option_underlyings)
    selection_calls, finalization_calls = [], []

    def claim(cursor, run_id, owner_id, **kwargs):
        underlyer = kwargs.get("underlyer") or next(underlyings, None)
        return (StagedOptionPackageClaim(run_id, underlyer, owner_id, 1, 1,
            MARKET_TIME + timedelta(minutes=5)) if underlyer else None)

    def complete(cursor, claim_row, receipt):
        receipts.append(receipt)

    def select(cursor, selection, **kwargs):
        selection_calls.append(selection)
        return option_manifest_selection_id(selection)

    runner, plan, request = _runner(source, claim_repository=claim,
        completion_repository=complete, receipt_loader=lambda cursor, active: tuple(receipts),
        usage_loader=lambda cursor, run_id: (source.requests, source.response_bytes, len(receipts), 0),
        selection_repository=select,
        finalization_repository=lambda cursor, active, selection_id, **kwargs:
            finalization_calls.append((selection_id, kwargs["finalized_at"])))

    assert runner.prepare(plan, collection_deadline=MARKET_TIME + timedelta(minutes=10)) == option_pre_session_run_id(request)
    owner = uuid5(NAMESPACE_URL, "staged-runner-owner")
    for ticker in plan.scope.universe.option_underlyings:
        receipt = runner.acquire_one(plan, owner, underlyer=ticker)
        assert receipt.underlyer == ticker and len(receipts) == len(selection_calls) + plan.scope.universe.option_underlyings.index(ticker) + 1
    result = runner.finalize(plan)

    assert result.plan.selected_contract_count == 128
    assert result.provider_requests == source.requests
    assert len(receipts) == len(result.discovery_batch_ids) == len(plan.scope.universe.option_underlyings)
    assert len(selection_calls) == len(finalization_calls) == 1


def test_staged_runner_records_failed_attempt_usage_without_persisting_package():
    plan = _target_plan()
    collection = _collection(plan)
    failed = plan.scope.universe.option_underlyings[0]
    source = StagedSource(collection.packages, fail=failed)
    package_calls, failures = [], []
    owner = uuid5(NAMESPACE_URL, "staged-failure-owner")
    request = _request_for(plan)
    claim = StagedOptionPackageClaim(option_pre_session_run_id(request), failed, owner, 1, 1,
        MARKET_TIME + timedelta(minutes=5))
    runner, plan, _ = _runner(source, claim_repository=lambda *args, **kwargs: claim,
        package_repository=lambda cursor, package: package_calls.append(package),
        failure_repository=lambda cursor, active, **kwargs: failures.append((active, kwargs)))

    with pytest.raises(ValueError, match="synthetic package failure"):
        runner.acquire_one(plan, owner, underlyer=failed)

    assert not package_calls
    assert failures[0][1]["provider_requests"] == 2
    assert failures[0][1]["response_bytes"] == collection.packages[0].evidence.response_bytes
    assert failures[0][1]["reason"] == "synthetic package failure"