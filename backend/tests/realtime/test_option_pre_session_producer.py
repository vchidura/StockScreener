import hashlib
import inspect
import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import pytest

from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryPlan,
    PolygonOptionChainDiscoveryResult,
)
from options.domain import BatchStatus, ContractType, PageValidationStatus, RawBatchPage, RawOptionBatch
from realtime.contracts import CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE
from realtime.option_discovery_repository import RealtimeOptionDiscoveryEvidence
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_manifest_repository import option_manifest_selection_id
from realtime.option_pre_session_producer import (
    CompletedOptionDiscoveryPackage,
    OptionPreSessionCollection,
    OptionPreSessionProducer,
    OptionPreSessionRequest,
)
import realtime.option_pre_session_producer as option_pre_session_producer
from realtime.option_session_manifest import ManifestSelectionStatus
from realtime.preflight import ProviderTier
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _advanced_policy,
    _advanced_universe,
    _candidates,
    _developer_policy,
    _scope,
)


class FakeSource:
    def __init__(self, collection):
        self.collection = collection
        self.requests = []

    def collect(self, request):
        self.requests.append(request)
        return self.collection


def _security_id(ticker):
    return uuid5(NAMESPACE_URL, f"pre-session-security:{ticker}")


def _packages(scope, policy, candidates):
    rows = []
    for ticker in scope.universe.option_underlyings:
        ticker_candidates = tuple(
            row for row in candidates if row.catalog.underlyer == ticker
        )
        batch_id = ticker_candidates[0].snapshot.batch_id
        spot = ticker_candidates[0].snapshot.spot
        plan = PolygonOptionChainDiscoveryPlan.build(
            ticker,
            MARKET_TIME,
            SESSION_DATE + timedelta(days=policy.maximum_dte),
            spot * (Decimal("1") - policy.strike_corridor_fraction),
            spot * (Decimal("1") + policy.strike_corridor_fraction),
        )
        response_bytes = json.dumps(
            {"request_id": f"pre-session-{ticker}", "results": [], "status": "OK"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        page = RawBatchPage(
            batch_id=batch_id,
            page_number=1,
            row_count=0,
            response_bytes=response_bytes,
            payload_sha256=hashlib.sha256(response_bytes).hexdigest(),
            received_at=MARKET_TIME + timedelta(seconds=1),
            terminal=True,
            validation_status=PageValidationStatus.VALID,
            request_filter_sha256=plan.request_filter_sha256,
            request_id=f"pre-session-{ticker}",
            request_metadata=(("path", plan.path),),
        )
        batch = RawOptionBatch(
            batch_id=batch_id,
            provider="polygon",
            underlyer=ticker,
            scheduled_cycle=MARKET_TIME,
            request_filter_sha256=plan.request_filter_sha256,
            policy_sha256=policy.sha256,
            status=BatchStatus.COMPLETE,
            pages=(page,),
            started_at=MARKET_TIME,
            completed_at=MARKET_TIME + timedelta(seconds=2),
        )
        discovery = PolygonOptionChainDiscoveryResult(
            batch,
            (),
            plan.included_ranges,
            plan.excluded_ranges,
        )
        evidence = RealtimeOptionDiscoveryEvidence(
            scope,
            _security_id(ticker),
            discovery,
        )
        rebound = tuple(
            replace(row, discovery_evidence_sha256=evidence.evidence_sha256)
            for row in ticker_candidates
        )
        rows.append(CompletedOptionDiscoveryPackage(evidence, rebound))
    return tuple(rows)


def _request(scope, policy, *, maximum_requests=None, maximum_bytes=None):
    return OptionPreSessionRequest(
        scope=scope,
        policy=policy,
        session_date=SESSION_DATE,
        scheduled_cycle=MARKET_TIME,
        effective_at=MARKET_TIME - timedelta(minutes=30),
        underlying_security_ids=tuple(
            (ticker, _security_id(ticker))
            for ticker in scope.universe.option_underlyings
        ),
        maximum_provider_requests=(
            maximum_requests
            if maximum_requests is not None
            else len(scope.universe.option_underlyings) * 40
        ),
        maximum_response_bytes=(
            maximum_bytes
            if maximum_bytes is not None
            else len(scope.universe.option_underlyings) * 67_108_864
        ),
    )


def _producer(collection):
    discoveries = []
    selections = []

    def persist_selection(plan, effective_at):
        selections.append((plan, effective_at))
        return option_manifest_selection_id(plan)

    producer = OptionPreSessionProducer(
        FakeSource(collection),
        persist_packages=lambda packages: discoveries.extend(
            package.evidence for package in packages
        ),
        persist_selection=persist_selection,
    )
    return producer, discoveries, selections


def test_developer_pre_session_producer_persists_exact_universe_then_ready_manifest():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(
        scope.universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    packages = _packages(scope, policy, candidates)
    response_bytes = sum(
        package.evidence.response_bytes for package in packages
    )
    collection = OptionPreSessionCollection(packages, 8, response_bytes)
    producer, discoveries, selections = _producer(collection)

    result = producer.run_once(_request(scope, policy))

    assert result.plan.status is ManifestSelectionStatus.READY
    assert result.plan.selected_contract_count == 128
    assert result.provider_requests == 8
    assert len(discoveries) == len(result.discovery_batch_ids) == 8
    assert [row.result.batch.underlyer for row in discoveries] == list(
        scope.universe.option_underlyings
    )
    assert selections == [(result.plan, MARKET_TIME - timedelta(minutes=30))]


def test_nine_name_preparation_retains_full_catalog_without_expanding_selected_subset():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(scope.universe, policy.target_dtes, 3)
    packages = _packages(scope, policy, candidates)
    collection = OptionPreSessionCollection(packages, 9, sum(row.evidence.response_bytes for row in packages))
    producer, discoveries, selections = _producer(collection)

    result = producer.run_once(_request(scope, policy))

    assert result.plan.status is ManifestSelectionStatus.READY
    assert result.plan.selected_contract_count == 144
    assert len(result.catalog) == len(result.plan.dispositions) == 216
    assert len(result.snapshots) == 216
    assert {row.contract_id for row in result.snapshots} == {row.contract_id for row in result.catalog}
    assert {row.underlyer for row in result.catalog} == set(DEVELOPER_FORWARD_UNIVERSE.option_underlyings)
    assert len(discoveries) == 9
    assert len(selections) == 1
    assert {row.contract_id for row in result.catalog} == {row.catalog.contract_id for row in candidates}


def test_nine_name_raw_discovery_is_bounded_per_underlying_not_by_aggregate_matrix_limit():
    from types import SimpleNamespace
    from realtime.forward_launcher import DeveloperForwardEnrollmentReceipt
    from realtime.option_manifest_cohort_repository import DeveloperForwardCohortBinding

    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(scope.universe, policy.target_dtes, 150)
    packages = _packages(scope, policy, candidates)
    collection = OptionPreSessionCollection(packages, 9, sum(row.evidence.response_bytes for row in packages))
    producer, discoveries, selections = _producer(collection)

    result = producer.run_once(_request(scope, policy))

    assert len(result.catalog) == len(result.snapshots) == len(result.plan.dispositions) == 10_800
    assert all(len(package.candidates) == 1_200 for package in packages)
    assert result.plan.status is ManifestSelectionStatus.READY
    assert result.plan.selected_contract_count == 144
    receipt = DeveloperForwardEnrollmentReceipt("a" * 64, result,
        DeveloperForwardCohortBinding("a" * 64, "b" * 64, "c" * 64), MARKET_TIME)
    window = SimpleNamespace(cutoff=MARKET_TIME + timedelta(minutes=15),
        available_after=MARKET_TIME + timedelta(minutes=30), deadline=MARKET_TIME + timedelta(minutes=31))
    matrix_scope = receipt.matrix_scope(window, reference_sha256="d" * 64, action_sha256="e" * 64)
    assert 144 < len(matrix_scope.catalog) <= 10_000
    assert len(discoveries) == 9 and len(selections) == 1
    assert {row.underlyer for row in matrix_scope.catalog} == set(DEVELOPER_FORWARD_UNIVERSE.option_underlyings)


def test_single_raw_underlying_over_native_limit_is_rejected_before_any_persistence():
    from tests.realtime.test_option_session_manifest import _candidate

    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = _developer_policy()
    packages = list(_packages(scope, policy, _candidates(scope.universe, policy.target_dtes, 2)))
    first = packages[0]
    oversized = tuple(replace(_candidate(first.evidence.result.batch.underlyer, 7, ContractType.CALL,
        Decimal("100") + Decimal(index) / Decimal("1000"), 1_000_000 + index),
        discovery_evidence_sha256=first.evidence.evidence_sha256) for index in range(10_001))
    packages[0] = replace(first, candidates=oversized)
    producer, discoveries, selections = _producer(OptionPreSessionCollection(tuple(packages), 9, 9_000))
    with pytest.raises(ValueError, match="underlying batch exceeds"):
        producer.run_once(_request(scope, policy))
    assert discoveries == [] and selections == []


def test_policy_admitted_global_matrix_limit_still_blocks_without_dropping_eligible_contracts():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(scope.universe, policy.target_dtes, 150, spot=Decimal("1000"))
    packages = _packages(scope, policy, candidates)
    producer, discoveries, selections = _producer(OptionPreSessionCollection(packages, 9, 9_000))
    with pytest.raises(ValueError, match="policy-admitted matrix exceeds"):
        producer.run_once(_request(scope, policy))
    assert discoveries == [] and selections == []


def test_advanced_pre_session_producer_preserves_50_underlying_800_slot_plan():
    universe = _advanced_universe(50)
    scope = _scope(ProviderTier.MASSIVE_ADVANCED, universe)
    policy = _advanced_policy()
    candidates = _candidates(
        universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
        spot=Decimal("350"),
    )
    packages = _packages(scope, policy, candidates)
    collection = OptionPreSessionCollection(
        packages,
        50,
        sum(package.evidence.response_bytes for package in packages),
    )
    producer, discoveries, selections = _producer(collection)

    result = producer.run_once(_request(scope, policy))

    assert result.plan.status is ManifestSelectionStatus.READY
    assert result.plan.selected_contract_count == 800
    assert len(discoveries) == 50
    assert len(selections) == 1


def test_missing_underlying_or_source_budget_fails_before_any_persistence():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(
        scope.universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    packages = _packages(scope, policy, candidates)

    missing_producer, missing_discoveries, missing_selections = _producer(
        OptionPreSessionCollection(packages[:-1], 7, 7_000)
    )
    with pytest.raises(ValueError, match="exact option universe"):
        missing_producer.run_once(_request(scope, policy))
    assert missing_discoveries == [] and missing_selections == []

    bounded_producer, bounded_discoveries, bounded_selections = _producer(
        OptionPreSessionCollection(packages, 9, 8_000)
    )
    with pytest.raises(ValueError, match="request bound"):
        bounded_producer.run_once(_request(scope, policy, maximum_requests=8))
    assert bounded_discoveries == [] and bounded_selections == []


def test_scope_security_and_candidate_lineage_drift_fail_closed():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = _candidates(
        scope.universe,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    packages = list(_packages(scope, policy, candidates))
    advanced_scope = RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_ADVANCED,
        packages[0].evidence.scope.profile_id,
        packages[0].evidence.scope.profile_sha256,
        scope.universe,
    )
    changed_evidence = RealtimeOptionDiscoveryEvidence(
        advanced_scope,
        packages[0].evidence.underlying_security_id,
        packages[0].evidence.result,
    )
    packages[0] = CompletedOptionDiscoveryPackage(
        changed_evidence,
        tuple(
            replace(
                candidate,
                discovery_evidence_sha256=changed_evidence.evidence_sha256,
            )
            for candidate in packages[0].candidates
        ),
    )
    producer, discoveries, selections = _producer(
        OptionPreSessionCollection(tuple(packages), 8, 8_000)
    )
    with pytest.raises(ValueError, match="scope drifted"):
        producer.run_once(_request(scope, policy))
    assert discoveries == [] and selections == []

    package = _packages(scope, policy, candidates)[0]
    with pytest.raises(ValueError, match="evidence lineage"):
        CompletedOptionDiscoveryPackage(
            package.evidence,
            (
                replace(
                    package.candidates[0],
                    discovery_evidence_sha256="0" * 64,
                ),
                *package.candidates[1:],
            ),
        )


def test_complete_discovery_can_persist_blocked_selection_without_partial_manifest():
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    policy = _developer_policy()
    candidates = tuple(
        row
        for row in _candidates(
            scope.universe,
            policy.target_dtes,
            policy.strikes_per_side_per_expiry,
        )
        if not (
            row.catalog.underlyer == "AAPL"
            and row.snapshot.calendar_dte == 60
            and row.catalog.contract_type is ContractType.PUT
        )
    )
    packages = _packages(scope, policy, candidates)
    collection = OptionPreSessionCollection(
        packages,
        8,
        sum(package.evidence.response_bytes for package in packages),
    )
    producer, discoveries, selections = _producer(collection)

    result = producer.run_once(_request(scope, policy))

    assert len(discoveries) == 8
    assert result.plan.status is ManifestSelectionStatus.BLOCKED
    assert result.plan.manifest is None
    assert selections == [(result.plan, MARKET_TIME - timedelta(minutes=30))]


def test_pre_session_composition_keeps_provider_and_database_boundaries_injected():
    source = inspect.getsource(option_pre_session_producer)

    for forbidden in (
        "psycopg2",
        "requests.",
        "worker_database",
        "from database",
        "import database",
        "backend.database",
        "options.repositories",
        "canonical_fallback",
    ):
        assert forbidden not in source
