from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from options.config import load_option_runtime_configuration

from realtime.contracts import CANARY_UNIVERSE, DEVELOPER_FORWARD_UNIVERSE
from realtime.option_pre_session_runtime import build_option_pre_session_runtime
from realtime.preflight import ProviderTier
from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler
from scripts.run_realtime_option_pre_session import build_developer_forward_process_plan, build_process_plan
from tests.realtime.test_option_session_manifest import MARKET_TIME


BACKEND_DIR = Path(__file__).resolve().parents[2]
PROFILE_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def test_forward_catalog_snapshots_are_bounded_distinct_and_preserve_observation_times():
    from dataclasses import replace
    from decimal import Decimal
    from options.calendar import OptionExchangeCalendar
    from options.domain import AssetType, OptionContractReference
    from realtime.option_pre_session_runtime import _admitted_catalog_entry

    reference = OptionContractReference(
        contract_ticker="O:AAPL261030C00100000", underlyer="AAPL", asset_type=AssetType.STOCK,
        provider="polygon", provider_version=None, provider_contract_type="call",
        expiration_date=MARKET_TIME.date() + timedelta(days=21), strike=Decimal("100"),
        provider_exercise_style="american", shares_per_contract=100, primary_exchange=None,
        correction=None, additional_underlyings_json="[]", adjustment_metadata_json="{}",
        changes_deliverables=False, valid_from=MARKET_TIME - timedelta(days=1),
        valid_to=None, first_observed_at=MARKET_TIME, revised_observed_at=None,
        refreshed_at=MARKET_TIME, payload_sha256="a" * 64)
    close = OptionExchangeCalendar().session_close(MARKET_TIME.date())
    historical = _admitted_catalog_entry(reference)
    first = _admitted_catalog_entry(reference, snapshot_expires_at=close)
    later_reference = replace(reference, first_observed_at=MARKET_TIME + timedelta(minutes=1),
        refreshed_at=MARKET_TIME + timedelta(minutes=1))
    later = _admitted_catalog_entry(later_reference, snapshot_expires_at=close)
    assert historical.catalog.valid_to is None
    assert first.catalog.valid_from == first.catalog.first_observed_at == MARKET_TIME
    assert first.catalog.valid_to == later.catalog.valid_to == close
    assert first.catalog.contract_id == later.catalog.contract_id == historical.catalog.contract_id
    assert len({row.catalog_version_id for row in (historical, first, later)}) == 3
    assert later.catalog.valid_from == later_reference.first_observed_at
    assert first == _admitted_catalog_entry(reference, snapshot_expires_at=close)
    shorter = replace(reference, valid_to=MARKET_TIME + timedelta(minutes=2))
    assert _admitted_catalog_entry(shorter, snapshot_expires_at=close).catalog.valid_to == shorter.valid_to
    with pytest.raises(ValueError, match="outside its source session"):
        _admitted_catalog_entry(reference, snapshot_expires_at=MARKET_TIME)
    revised = replace(reference, revised_observed_at=close, refreshed_at=close)
    with pytest.raises(ValueError, match="outside its source session"):
        _admitted_catalog_entry(revised, snapshot_expires_at=close)


def _environment():
    return {
        "POLYGON_API_KEY": "fake-only",
        "REALTIME_RUNTIME_DSN": "postgresql://fake-only",
        "OPTION_DATA_ENGINE": "polygon_developer",
        "OPTION_EQUITY_CONTEXT_ENABLED": "false",
        "OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED": "false",
        "OPTION_PACKAGE_ASSESSMENTS_ENABLED": "false",
        "OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED": "false",
        "OPTION_EXECUTION_ENGINE": "paper_proxy",
        "OPTION_UNIVERSE_MODE": "fixed",
        "OPTION_FIXED_STOCK_UNDERLYERS": "AAPL,MSFT,NVDA,AMZN,TSLA",
        "OPTION_FIXED_ETF_UNDERLYERS": "SPY,QQQ,IWM",
        "OPTION_STOCK_UNIVERSE_SIZE": "5",
        "OPTION_ETF_UNIVERSE_SIZE": "3",
        "OPTION_RAW_ARCHIVE_ENABLED": "false",
        "OPTION_START_READ_ONLY": "true",
        "OPTION_TRADE_INGESTION_ENABLED": "false",
    }


def _pool(fetches):
    cursor = MagicMock()
    cursor.fetchone.side_effect = fetches
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    return pool, connection, cursor


@pytest.mark.parametrize("forward_nine,native_transport", [(False, False), (True, False), (True, True)])
def test_runtime_factory_wires_verified_roles_profile_transport_and_cleanup(forward_nine, native_transport):
    from options.data.polygon_http import RequestsPolygonHttpTransport
    universe = DEVELOPER_FORWARD_UNIVERSE if forward_nine else CANARY_UNIVERSE
    environment = _environment()
    if forward_nine:
        environment.update(OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMZN,MSFT,NFLX,NVDA,TSLA", OPTION_STOCK_UNIVERSE_SIZE="6",
            OPTION_RISK_FREE_RATE_SOURCE="us_treasury_par_yield_v1",
            OPTION_DIVIDEND_INPUT_SOURCE="polygon_corporate_actions_v1")
    references_pool, _, _ = _pool((
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
        (
            str(PROFILE_ID),
            "d" * 64,
            ProviderTier.MASSIVE_DEVELOPER.value,
            universe.sha256,
        ),
    ))
    control_pool, _, _ = _pool((
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    ))
    pools = [references_pool, control_pool]
    transports = []

    def pool_factory(*args, **kwargs):
        return pools.pop(0)

    def transport_factory(api_key, **kwargs):
        transport = MagicMock()
        transports.append((api_key, kwargs, transport))
        return transport

    process_plan = (build_developer_forward_process_plan("stocks_realtime_v1", environ=environment,
        configuration=load_option_runtime_configuration(environment, BACKEND_DIR),
        maximum_provider_requests=360, maximum_response_bytes=33_554_432)
        if forward_nine else build_process_plan("stocks_realtime_v1", environ=environment))
    forward_bindings = dict(clock=lambda: MARKET_TIME,
        request_scheduler=DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=360),
            clock=lambda: MARKET_TIME, wait=MagicMock()), source_deadline=MARKET_TIME + timedelta(minutes=5)) if forward_nine else {}
    runtime = build_option_pre_session_runtime(
        process_plan,
        scheduled_cycle=MARKET_TIME,
        effective_at=MARKET_TIME - timedelta(minutes=30),
        environ=environment,
        backend_dir=BACKEND_DIR,
        pool_factory=pool_factory,
        transport_factory=RequestsPolygonHttpTransport if native_transport else transport_factory,
        profile_refresher=lambda *args: SimpleNamespace(
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            universe_sha256=universe.sha256,
            readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED")),
        ),
        **forward_bindings,
    )

    assert runtime.target_plan.scope.profile_id == PROFILE_ID
    assert runtime.target_plan.scope.universe == universe
    assert runtime.target_plan.policy.sha256 == process_plan.manifest_policy.sha256
    assert runtime.target_plan.maximum_provider_requests == (360 if forward_nine else 320)
    if native_transport:
        assert len(runtime.resources) == 1
        session = runtime.resources[0]
        assert session.get_adapter("https://provider.invalid").max_retries.total == 0
        session.close = MagicMock(wraps=session.close)
    else:
        assert len(transports) == 1
        assert transports[0][0] == "fake-only"
        transports[0][2].get.assert_not_called()

    runtime.close()

    references_pool.closeall.assert_called_once_with()
    control_pool.closeall.assert_called_once_with()
    if native_transport:
        session.close.assert_called_once()


@pytest.mark.parametrize("raw_policy", [False, True])
def test_runtime_bars_source_uses_the_valuation_policy_price_basis(monkeypatch, raw_policy):
    import json
    import realtime.option_pre_session_runtime as module
    from options.data.polygon_http import PolygonHttpResponse

    environment = _environment()
    environment.update(OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMZN,MSFT,NFLX,NVDA,TSLA", OPTION_STOCK_UNIVERSE_SIZE="6",
        OPTION_RISK_FREE_RATE_SOURCE="us_treasury_par_yield_v1",
        OPTION_DIVIDEND_INPUT_SOURCE="polygon_corporate_actions_v1")
    if raw_policy:
        environment["OPTION_VALUATION_POLICY_FILE"] = "options/policies/valuation_zero_yield_research_v1.json"
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    assert bool(configuration.valuation_policy.underlying_price_basis) is raw_policy
    universe = DEVELOPER_FORWARD_UNIVERSE
    references_pool, _, _ = _pool((("stocks_realtime_v1",), ("advanced_realtime_v1", "stocks_realtime_v1"),
        (str(PROFILE_ID), "d" * 64, ProviderTier.MASSIVE_DEVELOPER.value, universe.sha256)))
    control_pool, _, _ = _pool((("stocks_realtime_v1",), ("advanced_realtime_v1", "stocks_realtime_v1")))
    pools = [references_pool, control_pool]
    transports = []

    def transport_factory(api_key, **kwargs):
        transport = MagicMock()
        transports.append(transport)
        return transport

    captured = {}
    real_source = module.PolygonOptionPreSessionSource

    class SpySource(real_source):
        def __init__(self, *args, **kwargs):
            captured["bars_source"] = kwargs["bars_source"]
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(module, "PolygonOptionPreSessionSource", SpySource)
    process_plan = build_developer_forward_process_plan("stocks_realtime_v1", environ=environment,
        configuration=configuration, maximum_provider_requests=360, maximum_response_bytes=33_554_432)
    runtime = build_option_pre_session_runtime(
        process_plan, scheduled_cycle=MARKET_TIME, effective_at=MARKET_TIME - timedelta(minutes=30),
        environ=environment, backend_dir=BACKEND_DIR, pool_factory=lambda *a, **k: pools.pop(0),
        transport_factory=transport_factory,
        profile_refresher=lambda *args: SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            universe_sha256=universe.sha256, readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED"))),
        clock=lambda: MARKET_TIME,
        request_scheduler=DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=360),
            clock=lambda: MARKET_TIME, wait=MagicMock()), source_deadline=MARKET_TIME + timedelta(minutes=5))
    start = MARKET_TIME - timedelta(minutes=2)
    row = dict(t=int(start.timestamp() * 1000), o=100, h=100, l=100, c=100, v=10)
    transports[0].get.return_value = PolygonHttpResponse(
        200, json.dumps({"status": "OK", "results": [row]}).encode())

    result = captured["bars_source"]("AAPL", start, MARKET_TIME)

    params = transports[0].get.call_args.args[1]
    assert params["adjusted"] == ("false" if raw_policy else "true")
    assert result.coverage.price_basis == (
        "RAW_FINALIZED_MINUTE_CLOSE_V1" if raw_policy else "ADJUSTED_BAR_START_V1")
    assert result.provider_requests == 1
    # Raw finalized bars carry the closed-bar timestamp; legacy bars keep the start time.
    expected_time = start + timedelta(minutes=1) if raw_policy else start
    assert result.bars[0].market_data_time == expected_time
    runtime.close()


def test_prepared_target_plan_reuses_exact_scope_without_profile_refresh():
    environment, configuration, process_plan = _forward_enrollment_inputs()
    references_pool, _, _ = _pool((("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
        (str(PROFILE_ID), "d" * 64, ProviderTier.MASSIVE_DEVELOPER.value,
            DEVELOPER_FORWARD_UNIVERSE.sha256)))
    control_pool, _, _ = _pool((("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1")))
    pools = [references_pool, control_pool]
    bindings = dict(scheduled_cycle=MARKET_TIME, effective_at=MARKET_TIME,
        environ=environment, backend_dir=BACKEND_DIR, pool_factory=lambda *args, **kwargs: pools.pop(0),
        transport_factory=lambda *args, **kwargs: MagicMock(), clock=lambda: MARKET_TIME,
        request_scheduler=DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=360),
            clock=lambda: MARKET_TIME, wait=MagicMock()), source_deadline=MARKET_TIME + timedelta(minutes=5))
    runtime = build_option_pre_session_runtime(process_plan,
        profile_refresher=lambda *args: SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            universe_sha256=DEVELOPER_FORWARD_UNIVERSE.sha256,
            readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED"))), **bindings)
    prepared = runtime.target_plan
    runtime.close()

    references_pool, _, _ = _pool((("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1")))
    control_pool, _, _ = _pool((("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1")))
    pools = [references_pool, control_pool]
    bindings["pool_factory"] = lambda *args, **kwargs: pools.pop(0)
    refresher = MagicMock(side_effect=AssertionError("prepared scope refreshed profile"))
    resumed = build_option_pre_session_runtime(process_plan,
        prepared_target_plan=prepared, profile_refresher=refresher, **bindings)

    assert resumed.target_plan == prepared
    refresher.assert_not_called()
    resumed.close()


def test_metered_transport_counts_failed_attempts_and_stops_before_budget_overrun():
    from realtime.option_pre_session_runtime import MeteredPolygonTransport

    delegate = MagicMock()
    delegate.get.side_effect = TimeoutError("synthetic timeout")
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=1, maximum_attempts=1),
        clock=lambda: MARKET_TIME, wait=MagicMock())
    transport = MeteredPolygonTransport(delegate, scheduler=scheduler, available_after=MARKET_TIME,
        deadline=MARKET_TIME + timedelta(seconds=90))
    with pytest.raises(ValueError, match="timeout retry budget exhausted"):
        transport.get("https://provider.invalid", {})
    assert transport.snapshot() == (1, 0)
    with pytest.raises(ValueError, match="budget exhausted"):
        transport.get("https://provider.invalid", {})
    delegate.get.assert_called_once()


def _forward_enrollment_inputs():
    environment = _environment()
    environment.update(REALTIME_RUNTIME_DSN="dbname=stocks_realtime_v1 user=stocks_realtime_runtime",
        OPTION_FIXED_STOCK_UNDERLYERS="AAPL,AMZN,MSFT,NFLX,NVDA,TSLA", OPTION_STOCK_UNIVERSE_SIZE="6",
        OPTION_RISK_FREE_RATE_SOURCE="us_treasury_par_yield_v1",
        OPTION_DIVIDEND_INPUT_SOURCE="polygon_corporate_actions_v1")
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    plan = build_developer_forward_process_plan("stocks_realtime_v1", environ=environment,
        configuration=configuration, maximum_provider_requests=360, maximum_response_bytes=33_554_432)
    return environment, configuration, plan


def test_forward_enrollment_caller_is_inert_and_rejects_expired_window():
    from realtime.forward_launcher import DeveloperForwardEnrollmentLauncher

    environment, configuration, plan = _forward_enrollment_inputs()
    factory, pools, transport = MagicMock(), MagicMock(), MagicMock()
    launcher = DeveloperForwardEnrollmentLauncher(plan, configuration, environ=environment,
        backend_dir=BACKEND_DIR, session_date=MARKET_TIME.date(), deadline=MARKET_TIME,
        pool_factory=pools, clock=lambda: MARKET_TIME, wait=MagicMock(),
        server_max_connections=20, reserved_connections=5, runtime_factory=factory, transport_factory=transport)
    factory.assert_not_called()
    pools.assert_not_called()
    transport.assert_not_called()
    with pytest.raises(ValueError, match="session/deadline"):
        launcher.run_once()
    assert launcher.last_provider_requests == launcher.last_response_bytes == 0
    factory.assert_not_called()


@pytest.mark.parametrize("failure", [None, "source", "ledger", "profile", "blocked"])
def test_forward_enrollment_caller_runs_full_nine_catalog_and_closes_runtime(failure):
    from dataclasses import replace
    from realtime.forward_launcher import DeveloperForwardEnrollmentLauncher
    from realtime.forward_runtime import DeveloperForwardWindow
    from realtime.option_pre_session_producer import OptionPreSessionCollection
    from tests.realtime.test_option_pre_session_producer import _packages, _producer, _request
    from tests.realtime.test_option_session_manifest import _candidates, _scope

    environment, configuration, plan = _forward_enrollment_inputs()
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    candidates = _candidates(scope.universe, plan.manifest_policy.target_dtes, 3)
    packages = _packages(scope, plan.manifest_policy, candidates)
    if failure == "blocked":
        packages = tuple(replace(package, candidates=()) for package in packages)
    provider_bytes = sum(row.evidence.response_bytes for row in packages)
    collection = OptionPreSessionCollection(packages, 9, provider_bytes)
    producer, discoveries, selections = _producer(collection)
    current_time = [MARKET_TIME]
    closed = MagicMock()
    calls = []

    def wait(seconds):
        current_time[0] += timedelta(seconds=seconds)

    def runtime_factory(process_plan, **bindings):
        calls.append(bindings)
        assert process_plan == plan
        def run_once(target_plan):
            if failure == "source":
                raise RuntimeError("synthetic source failure")
            scheduler = bindings["request_scheduler"]
            for package in packages:
                scheduler.request(lambda: SimpleNamespace(body=b"x" * package.evidence.response_bytes),
                    available_after=bindings["scheduled_cycle"], deadline=bindings["source_deadline"])
            request = replace(_request(scope, plan.manifest_policy), scheduled_cycle=bindings["scheduled_cycle"],
                effective_at=bindings["effective_at"])
            result = producer.run_once(request)
            if failure == "ledger":
                return replace(result, provider_requests=10)
            if failure == "profile":
                return replace(result, plan=replace(result.plan, scope=replace(scope, profile_id=PROFILE_ID)))
            return result
        return SimpleNamespace(runner=SimpleNamespace(run_once=run_once), target_plan=SimpleNamespace(scope=scope), close=closed)

    launcher = DeveloperForwardEnrollmentLauncher(plan, configuration, environ=environment,
        backend_dir=BACKEND_DIR, session_date=MARKET_TIME.date(), deadline=MARKET_TIME + timedelta(minutes=5),
        pool_factory=MagicMock(), clock=lambda: current_time[0], wait=wait,
        server_max_connections=20, reserved_connections=5, runtime_factory=runtime_factory)
    if failure not in (None, "blocked"):
        with pytest.raises((ValueError, RuntimeError)):
            launcher.run_once()
    else:
        receipt = launcher.run_once()
        assert receipt.effective_at == MARKET_TIME
        window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
            MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
        if failure == "blocked":
            assert receipt.cohort_id is None
            assert receipt.result.catalog == ()
            with pytest.raises(ValueError, match="complete ready enrollment"):
                receipt.matrix_scope(window, reference_sha256="a" * 64, action_sha256="b" * 64)
        else:
            assert receipt.result.plan.selected_contract_count == 144
            assert len(receipt.result.catalog) == 216
            assert receipt.cohort_id not in {PROFILE_ID, None}
            matrix_scope = receipt.matrix_scope(window, reference_sha256="a" * 64, action_sha256="b" * 64)
            assert len(matrix_scope.catalog) == 216
            assert matrix_scope.configuration_sha256 == configuration.configuration_sha256
            assert matrix_scope.cohort_id == receipt.cohort_id
        assert len(discoveries) == 9 and len(selections) == 1
    closed.assert_called_once()
    assert len(calls) == 1
    with pytest.raises(ValueError, match="reactivate"):
        launcher.run_once()


@pytest.mark.parametrize("failure", ["second_pool", "shared_pool"])
def test_preparation_factory_closes_owned_pool_when_second_pool_fails(failure):
    first_pool = MagicMock()
    pool_factory = MagicMock(side_effect=[first_pool, RuntimeError("second pool failed")]) if failure == "second_pool" else MagicMock(return_value=first_pool)
    with pytest.raises((ValueError, RuntimeError)):
        build_option_pre_session_runtime(build_process_plan("stocks_realtime_v1", environ=_environment()),
            scheduled_cycle=MARKET_TIME, effective_at=MARKET_TIME, environ=_environment(), backend_dir=BACKEND_DIR,
            pool_factory=pool_factory, profile_refresher=lambda *args: SimpleNamespace(
                provider_tier=ProviderTier.MASSIVE_DEVELOPER, universe_sha256=CANARY_UNIVERSE.sha256,
                readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED"))))
    first_pool.closeall.assert_called_once()


def test_forward_enrollment_invalid_dsn_is_redacted_before_any_factory_call():
    from realtime.forward_launcher import DeveloperForwardEnrollmentLauncher

    environment, configuration, plan = _forward_enrollment_inputs()
    environment["REALTIME_RUNTIME_DSN"] = "invalid-fixture-secret"
    factory = MagicMock()
    with pytest.raises(ValueError, match="DSN is invalid") as error:
        DeveloperForwardEnrollmentLauncher(plan, configuration, environ=environment,
            backend_dir=BACKEND_DIR, session_date=MARKET_TIME.date(), deadline=MARKET_TIME + timedelta(minutes=5),
            pool_factory=factory, clock=MagicMock(), wait=MagicMock(),
            server_max_connections=20, reserved_connections=5, runtime_factory=factory)
    assert "invalid-fixture-secret" not in str(error.value)
    factory.assert_not_called()


def _ready_forward_receipt(rejected_extra=False):
    from dataclasses import replace
    from realtime.forward_launcher import DeveloperForwardEnrollmentReceipt
    from realtime.option_pre_session_producer import OptionPreSessionCollection
    from tests.realtime.test_option_pre_session_producer import _packages, _producer, _request
    from tests.realtime.test_option_session_manifest import _candidates, _scope

    environment, configuration, plan = _forward_enrollment_inputs()
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, DEVELOPER_FORWARD_UNIVERSE)
    candidates = _candidates(scope.universe, plan.manifest_policy.target_dtes, 3)
    if rejected_extra:
        last = candidates[-1]
        candidates = (*candidates[:-1], replace(last, snapshot=replace(last.snapshot, day_volume=0, open_interest=0)))
    packages = _packages(scope, plan.manifest_policy, candidates)
    producer, _, _ = _producer(OptionPreSessionCollection(packages, 9, sum(row.evidence.response_bytes for row in packages)))
    result = producer.run_once(_request(scope, plan.manifest_policy))
    return configuration, DeveloperForwardEnrollmentReceipt(plan.sha256, result, plan.cohort_binding, MARKET_TIME)


def test_receipt_keeps_rejected_discovery_evidence_but_matrix_requires_only_policy_admitted_catalog():
    from realtime.forward_runtime import DeveloperForwardWindow

    configuration, receipt = _ready_forward_receipt(rejected_extra=True)
    window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
        MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
    scope = receipt.matrix_scope(window, reference_sha256="a" * 64, action_sha256="b" * 64)
    assert receipt.result.plan.selected_contract_count == 144
    assert len(receipt.result.catalog) == 216
    assert len(scope.catalog) == 215
    assert {row.contract_id for row in scope.catalog} < {row.contract_id for row in receipt.result.catalog}


@pytest.mark.parametrize("drift", [None, "absent", "configuration", "manifest", "profile", "tier"])
def test_enrolled_cycle_checks_target_binding_before_any_launcher_or_provider(monkeypatch, drift):
    from contextlib import contextmanager
    from realtime.forward_launcher import run_developer_forward_enrolled_cycle
    from realtime.forward_runtime import DeveloperForwardWindow
    from realtime.option_manifest_repository import option_subscription_manifest_id
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime.database_access import RealtimeDatabaseRole

    configuration, receipt = _ready_forward_receipt()
    binding = receipt.cohort_binding
    row = [str(option_subscription_manifest_id(receipt.result.plan)), binding.configuration_sha256,
        binding.model_bundle_sha256, binding.valuation_sha256, "SHADOW", str(receipt.result.plan.scope.profile_id),
        "MASSIVE_DEVELOPER", "MASSIVE_DEVELOPER", "MASSIVE_DEVELOPER", "DEVELOPER_READY",
        receipt.result.plan.manifest.sha256, configuration.valuation_policy_sha256, MARKET_TIME, MARKET_TIME]
    if drift in {"configuration", "manifest", "profile", "tier"}:
        row[{"configuration": 1, "manifest": 0, "profile": 5, "tier": 6}[drift]] = "wrong"
    cursor = MagicMock()
    cursor.fetchone.return_value = None if drift == "absent" else tuple(row)
    transactions = []

    class Database(RealtimeProcessDatabase):
        def __init__(self):
            self.plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)

        @property
        def process_plan(self):
            return self.plan

        @contextmanager
        def transaction(self, **options):
            transactions.append(options)
            yield cursor

    run_once = MagicMock(return_value="synthetic-release")
    factory = MagicMock(return_value=SimpleNamespace(run_once=run_once))
    monkeypatch.setattr("realtime.forward_launcher.build_developer_forward_launcher", factory)
    window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
        MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
    arguments = dict(verification_database=Database(), window=window, reference_sha256="a" * 64, action_sha256="b" * 64)
    if drift:
        with pytest.raises(ValueError, match="absent or drifted"):
            run_developer_forward_enrolled_cycle(receipt, configuration, **arguments)
        factory.assert_not_called()
        run_once.assert_not_called()
    else:
        assert run_developer_forward_enrolled_cycle(receipt, configuration, **arguments) == "synthetic-release"
        factory.assert_called_once()
        run_once.assert_called_once()
        assert factory.call_args.args[0].cohort_id == receipt.cohort_id
        assert len(factory.call_args.args[0].catalog) == 216
    assert transactions == [dict(read_only=True, isolation_level="REPEATABLE READ")]
    assert cursor.execute.call_args.args[1] == (str(receipt.cohort_id),)


@pytest.mark.parametrize("defect", ["release", "deadline", "expiry"])
def test_enrolled_cycle_rejects_unreviewed_windows_before_target_or_provider(monkeypatch, defect):
    from dataclasses import replace
    from contextlib import contextmanager
    from realtime.forward_launcher import run_developer_forward_enrolled_cycle
    from realtime.forward_runtime import DeveloperForwardWindow
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime.database_access import RealtimeDatabaseRole

    configuration, receipt = _ready_forward_receipt()
    window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
        MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
    window = replace(window, **{
        "release": {"available_after": window.available_after - timedelta(seconds=1)},
        "deadline": {"deadline": window.available_after + timedelta(seconds=91)},
        "expiry": {"serving_expires_at": window.serving_expires_at + timedelta(days=1)},
    }[defect])
    transactions = []

    class Database(RealtimeProcessDatabase):
        def __init__(self):
            self.plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)

        @property
        def process_plan(self):
            return self.plan

        @contextmanager
        def transaction(self, **options):
            transactions.append(options)
            yield MagicMock()

    factory = MagicMock()
    monkeypatch.setattr("realtime.forward_launcher.build_developer_forward_launcher", factory)
    with pytest.raises(ValueError, match="reviewed"):
        run_developer_forward_enrolled_cycle(receipt, configuration, verification_database=Database(),
            window=window, reference_sha256="a" * 64, action_sha256="b" * 64)
    assert transactions == []
    factory.assert_not_called()


def test_combined_bootstrap_plan_is_inert_exact_and_requires_sufficient_requests():
    from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
    from realtime.forward_bootstrap import build_developer_forward_bootstrap_plan

    configuration, receipt = _ready_forward_receipt()
    arguments = dict(history_session=receipt.result.plan.session_date - timedelta(days=3),
        maximum_requests=450, maximum_response_bytes=512 * 1024 * 1024,
        deadline=MARKET_TIME + timedelta(hours=1))
    plan = build_developer_forward_bootstrap_plan(receipt, **arguments)
    ticker_count = len(receipt.result.plan.scope.universe.equity_tickers)
    history = build_realtime_equity_portal_history_plan(arguments["history_session"], equity_ticker_count=ticker_count)
    assert plan.enrollment.cohort_id == receipt.cohort_id
    assert plan.expected_history_requests == history.expected_provider_requests == 45
    assert plan.expected_history_facts == history.expected_total_facts
    assert len(plan.sha256) == 64
    minimum_requests = history.expected_provider_requests + 2 * ticker_count
    assert build_developer_forward_bootstrap_plan(receipt,
        **dict(arguments, maximum_requests=minimum_requests)).maximum_requests == minimum_requests
    with pytest.raises(ValueError, match="cannot cover history"):
        build_developer_forward_bootstrap_plan(receipt, **dict(arguments, maximum_requests=minimum_requests - 1))
    with pytest.raises(ValueError, match="prior completed history"):
        build_developer_forward_bootstrap_plan(receipt, **dict(arguments, history_session=receipt.result.plan.session_date))


def test_bootstrap_oi_uses_complete_admitted_snapshot_evidence_and_actual_receipt_time():
    from realtime.forward_bootstrap import dated_open_interest_facts

    configuration, receipt = _ready_forward_receipt(rejected_extra=True)
    received_at = MARKET_TIME + timedelta(seconds=30)
    facts = dated_open_interest_facts(receipt, received_at=received_at)
    assert len(facts) == 215
    assert {dict(row.values)["contract_id"] for row in facts} < {row.contract_id for row in receipt.result.catalog}
    assert all(dict(row.values)["received_at"] == received_at for row in facts)
    assert all(dict(row.values)["source"] == "massive" for row in facts)
    assert all(dict(row.values)["date_basis"] == "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION" for row in facts)


def test_concrete_bootstrap_sources_are_inert_single_attempt_and_share_one_ledger():
    from realtime.forward_bootstrap import build_developer_forward_bootstrap_plan, build_developer_forward_bootstrap_sources
    from tests.realtime.test_equity_validation_source import _identities

    configuration, receipt = _ready_forward_receipt()
    plan = build_developer_forward_bootstrap_plan(receipt, history_session=receipt.result.plan.session_date - timedelta(days=3),
        maximum_requests=450, maximum_response_bytes=512 * 1024 * 1024, deadline=MARKET_TIME + timedelta(hours=1))
    session = MagicMock()
    session.get_adapter.return_value.max_retries.total = 0
    sources = build_developer_forward_bootstrap_sources(plan, _identities(),
        environ={"POLYGON_API_KEY": "fixture-only", "FINNHUB_API_KEY": "fixture-only"},
        clock=lambda: MARKET_TIME, wait=MagicMock(), session_factory=lambda: session)
    session.get.assert_not_called()
    assert sources.history_source.transport.scheduler is sources.ledger
    assert sources.history_source.cohort_id == receipt.cohort_id
    assert sources.history_source.limits.maximum_workers == 1
    assert sources.history_source.limits.maximum_retries_per_request == 0
    assert sources.ledger.requests == 0
    sources.close()
    session.close.assert_called_once()


@pytest.mark.parametrize("defect", [None, "references", "cohort"])
def test_concrete_historical_bootstrap_persists_native_stages_without_readiness(monkeypatch, defect):
    from contextlib import contextmanager
    from dataclasses import replace
    from realtime import forward_bootstrap as bootstrap
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime.reference_seed import SeedStatus

    configuration, receipt = _ready_forward_receipt()
    plan = bootstrap.build_developer_forward_bootstrap_plan(receipt,
        history_session=receipt.result.plan.session_date - timedelta(days=3), maximum_requests=450,
        maximum_response_bytes=512 * 1024 * 1024, deadline=MARKET_TIME + timedelta(hours=1))
    from realtime.reference_service import ReferenceSeedService
    from realtime.equity_validation_source import PolygonEquityValidationHistorySource
    references = MagicMock(spec=ReferenceSeedService)
    references.run_once.return_value = SimpleNamespace(manifest=SimpleNamespace(
        status=SeedStatus.UNAVAILABLE if defect == "references" else SeedStatus.READY,
        universe_sha256=bootstrap.EQUITY_VALIDATION_UNIVERSE.sha256,
        session_date=receipt.result.plan.session_date, seed_sha256="a" * 64), facts=(SimpleNamespace(relation=bootstrap.ReferenceFactRelation.SECURITY),))
    history = MagicMock(spec=PolygonEquityValidationHistorySource)
    history.cohort_id = receipt.cohort_id
    history.universe = bootstrap.DEVELOPER_FORWARD_UNIVERSE
    per_ticker = tuple(SimpleNamespace(cohort_id=PROFILE_ID if defect == "cohort" else receipt.cohort_id,
        revision=SimpleNamespace(ticker=ticker)) for ticker in bootstrap.DEVELOPER_FORWARD_UNIVERSE.equity_tickers)
    facts = per_ticker * (plan.expected_history_facts // 9)
    history.collect_portal_history.return_value = SimpleNamespace(plan=SimpleNamespace(session_date=plan.history_session), facts=facts)
    sources = bootstrap.DeveloperForwardBootstrapSources(references, history,
        SimpleNamespace(requests=42, response_bytes=1024), MagicMock())
    cursor = MagicMock()
    transactions = []

    class Database(RealtimeProcessDatabase):
        def __init__(self, role):
            self.plan = SimpleNamespace(role=role)

        @property
        def process_plan(self):
            return self.plan

        @contextmanager
        def transaction(self, **options):
            transactions.append((self.plan.role, options))
            yield cursor

    monkeypatch.setattr(bootstrap, "_verify_enrollment_cursor", MagicMock())
    reference_writer = MagicMock()
    bar_writer = MagicMock()
    tier_writer = MagicMock()
    monkeypatch.setattr(bootstrap, "persist_reference_facts_cursor", reference_writer)
    monkeypatch.setattr(bootstrap, "persist_equity_validation_facts_cursor", bar_writer)
    monkeypatch.setattr(bootstrap, "persist_tier_cohort_binding_cursor", tier_writer)
    arguments = dict(references_database=Database(RealtimeDatabaseRole.REFERENCES),
        compute_database=Database(RealtimeDatabaseRole.COMPUTE), sources=sources,
        split_coverage={}, split_actions=(), clock=lambda: MARKET_TIME + timedelta(seconds=30))
    if defect:
        with pytest.raises(ValueError, match="incomplete" if defect == "references" else "changed target"):
            bootstrap.run_developer_forward_historical_bootstrap(plan, **arguments)
        bar_writer.assert_not_called()
        if defect == "references":
            reference_writer.assert_not_called()
            history.collect_portal_history.assert_not_called()
    else:
        report = bootstrap.run_developer_forward_historical_bootstrap(plan, **arguments)
        assert report["status"] == "HISTORICAL_WARMUP_STORED_NOT_DATA_READY"
        assert report["readiness_persisted"] is False
        assert report["historical_fact_count"] == plan.expected_history_facts
        assert report["oi_contract_count"] == 216
        assert report["reference_sha256"] == "a" * 64
        assert len(report["action_sha256"]) == 64
        bar_writer.assert_called_once_with(cursor, facts)
        assert reference_writer.call_count == 2
        assert report["provider_requests"] == 42
    tier_writer.assert_not_called()
    assert transactions[0] == (RealtimeDatabaseRole.COMPUTE, dict(read_only=True, isolation_level="REPEATABLE READ"))


@pytest.mark.parametrize("late", [False, True])
def test_current_spot_context_phase_uses_narrow_cutoff_deadline_and_never_writes_readiness(monkeypatch, late):
    from contextlib import contextmanager
    from realtime import forward_bootstrap as bootstrap
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    from realtime.forward_runtime import DeveloperForwardWindow

    configuration, receipt = _ready_forward_receipt()
    plan = bootstrap.build_developer_forward_bootstrap_plan(receipt,
        history_session=receipt.result.plan.session_date - timedelta(days=3), maximum_requests=450,
        maximum_response_bytes=512 * 1024 * 1024, deadline=MARKET_TIME + timedelta(hours=1))
    window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
        MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
    scope = receipt.matrix_scope(window, reference_sha256="a" * 64, action_sha256="b" * 64)
    http = SimpleNamespace(deadline=plan.deadline)
    source = SimpleNamespace(cohort_id=receipt.cohort_id, transport=http)
    minute_facts = (SimpleNamespace(cohort_id=receipt.cohort_id),)
    context_facts = (SimpleNamespace(cohort_id=receipt.cohort_id),)
    observed_deadlines = []
    def collect_minute(cutoff):
        observed_deadlines.append(http.deadline)
        return minute_facts
    source.collect_forward_minute_inputs = collect_minute
    source.collect_forward_context_inputs = lambda cutoff: context_facts
    sources = bootstrap.DeveloperForwardBootstrapSources(MagicMock(), source,
        SimpleNamespace(requests=18, response_bytes=1024), MagicMock())
    writer, tier_writer = MagicMock(), MagicMock()
    monkeypatch.setattr(bootstrap, "persist_equity_validation_facts_cursor", writer)
    monkeypatch.setattr(bootstrap, "persist_tier_cohort_binding_cursor", tier_writer)
    cursor = MagicMock()

    class Database(RealtimeProcessDatabase):
        def __init__(self):
            self.plan = SimpleNamespace(role=RealtimeDatabaseRole.COMPUTE)

        @property
        def process_plan(self):
            return self.plan

        @contextmanager
        def transaction(self, **options):
            yield cursor

    arguments = dict(scope=scope, compute_database=Database(), sources=sources,
        clock=lambda: scope.deadline if late else scope.available_after)
    if late:
        with pytest.raises(ValueError, match="missed the cutoff"):
            bootstrap.run_developer_forward_current_spot_bootstrap(plan, **arguments)
        writer.assert_not_called()
    else:
        report = bootstrap.run_developer_forward_current_spot_bootstrap(plan, **arguments)
        assert report["status"] == "CURRENT_SPOTS_CONTEXT_STORED_NOT_DATA_READY"
        assert report["readiness_persisted"] is False
        writer.assert_called_once_with(cursor, minute_facts + context_facts)
    assert observed_deadlines == [scope.deadline]
    assert http.deadline == plan.deadline
    tier_writer.assert_not_called()


@pytest.mark.parametrize("defect", [None, "context", "oi", "valuation", "deadline"])
def test_bootstrap_acceptance_writes_only_after_complete_native_input_validation(monkeypatch, defect):
    from contextlib import contextmanager
    from realtime import forward_bootstrap as bootstrap
    from realtime.forward_runtime import DeveloperForwardWindow
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase

    configuration, receipt = _ready_forward_receipt()
    window = DeveloperForwardWindow(MARKET_TIME + timedelta(minutes=15), MARKET_TIME + timedelta(minutes=30),
        MARKET_TIME + timedelta(minutes=31), MARKET_TIME + timedelta(minutes=46))
    scope = receipt.matrix_scope(window, reference_sha256="a" * 64, action_sha256="b" * 64)
    cursor = MagicMock()
    transactions = []

    class Database(RealtimeProcessDatabase):
        def __init__(self, role):
            self.plan = SimpleNamespace(role=role)

        @property
        def process_plan(self):
            return self.plan

        @contextmanager
        def transaction(self, **options):
            transactions.append((self.plan.role, options))
            yield cursor

    monkeypatch.setattr(bootstrap, "_database_clock", lambda cursor: window.deadline if defect == "deadline" else window.available_after)
    def collection_facts(*args, **kwargs):
        if defect == "valuation":
            raise ValueError("stored valuation provenance is unavailable")
        return {"security_id": PROFILE_ID}
    monkeypatch.setattr(bootstrap, "load_developer_collection_facts_cursor", collection_facts)
    bars = tuple((SimpleNamespace(interval="30m"), MARKET_TIME) for _ in range(202)) + tuple(
        (SimpleNamespace(interval="1d"), MARKET_TIME) for _ in range(21))
    monkeypatch.setattr(bootstrap, "_load_context_bars", lambda *args: () if defect == "context" else bars)
    monkeypatch.setattr(bootstrap, "_load_split_coverage", lambda *args: ("native-coverage",))
    monkeypatch.setattr(bootstrap, "_load_open_interest", lambda *args: () if defect == "oi" else tuple(
        SimpleNamespace(record=SimpleNamespace(contract_id=row.contract_id)) for row in scope.catalog))
    verify = MagicMock()
    persist = MagicMock()
    monkeypatch.setattr(bootstrap, "_verify_enrollment_cursor", verify)
    monkeypatch.setattr(bootstrap, "persist_tier_cohort_binding_cursor", persist)
    arguments = dict(scope=scope, compute_database=Database(RealtimeDatabaseRole.COMPUTE),
        control_database=Database(RealtimeDatabaseRole.CONTROL))
    if defect is None:
        binding = bootstrap.accept_developer_forward_bootstrap(receipt, configuration, **arguments)
        assert binding.cohort_id == receipt.cohort_id
        assert binding.source_manifest_sha256 == receipt.result.plan.manifest.sha256
        assert binding.effective_at == window.available_after
        persist.assert_called_once_with(cursor, binding)
        verify.assert_called_once_with(cursor, receipt)
        assert transactions[-1] == (RealtimeDatabaseRole.CONTROL, {})
    else:
        error = {"context": "live-mode trend/liquidity/split", "oi": "OI coverage",
            "valuation": "stored valuation provenance", "deadline": "cutoff deadline"}[defect]
        with pytest.raises(ValueError, match=error):
            bootstrap.accept_developer_forward_bootstrap(receipt, configuration, **arguments)
        persist.assert_not_called()
        verify.assert_not_called()
        assert len(transactions) == 1
    assert transactions[0] == (RealtimeDatabaseRole.COMPUTE,
        dict(dict_cursor=True, read_only=True, isolation_level="REPEATABLE READ"))


def test_current_input_plan_has_separate_tight_budget_from_historical_bootstrap():
    from realtime.forward_bootstrap import build_developer_forward_current_plan

    _, receipt = _ready_forward_receipt()
    plan = build_developer_forward_current_plan(receipt, maximum_requests=36,
        maximum_response_bytes=67_108_864, deadline=MARKET_TIME + timedelta(hours=1))
    assert plan.maximum_requests == 36
    assert plan.maximum_response_bytes == 67_108_864
    assert len(plan.sha256) == 64
    for changes in ({"maximum_requests": 17}, {"maximum_requests": 37},
            {"maximum_response_bytes": 67_108_865}):
        with pytest.raises(ValueError, match="budgets"):
            build_developer_forward_current_plan(receipt, deadline=MARKET_TIME + timedelta(hours=1),
                maximum_requests=changes.get("maximum_requests", 36),
                maximum_response_bytes=changes.get("maximum_response_bytes", 67_108_864))


def test_runtime_factory_requires_secrets_before_pool_or_transport_construction():
    pool_factory = MagicMock()
    transport_factory = MagicMock()

    with pytest.raises(RuntimeError, match="REALTIME_RUNTIME_DSN and POLYGON_API_KEY"):
        build_option_pre_session_runtime(
            build_process_plan("stocks_realtime_v1", environ=_environment()),
            scheduled_cycle=MARKET_TIME,
            effective_at=MARKET_TIME - timedelta(minutes=30),
            environ={},
            backend_dir=BACKEND_DIR,
            pool_factory=pool_factory,
            transport_factory=transport_factory,
            profile_refresher=MagicMock(),
        )

    pool_factory.assert_not_called()
    transport_factory.assert_not_called()


def test_runtime_factory_rejects_cycle_outside_stable_mark_window_before_side_effects():
    pool_factory = MagicMock()
    transport_factory = MagicMock()
    profile_refresher = MagicMock()

    with pytest.raises(ValueError, match="stable mark freshness"):
        build_option_pre_session_runtime(
            build_process_plan("stocks_realtime_v1", environ=_environment()),
            scheduled_cycle=MARKET_TIME - timedelta(hours=13),
            effective_at=MARKET_TIME - timedelta(hours=13, minutes=30),
            environ=_environment(),
            backend_dir=BACKEND_DIR,
            pool_factory=pool_factory,
            transport_factory=transport_factory,
            profile_refresher=profile_refresher,
        )

    profile_refresher.assert_not_called()
    pool_factory.assert_not_called()
    transport_factory.assert_not_called()


def test_runtime_factory_closes_both_pools_when_profile_is_unavailable():
    references_pool, _, _ = _pool((
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
        None,
    ))
    control_pool, _, _ = _pool((
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    ))
    pools = [references_pool, control_pool]

    with pytest.raises(ValueError, match="current blocked Developer profile"):
        build_option_pre_session_runtime(
            build_process_plan("stocks_realtime_v1", environ=_environment()),
            scheduled_cycle=MARKET_TIME,
            effective_at=MARKET_TIME - timedelta(minutes=30),
            environ=_environment(),
            backend_dir=BACKEND_DIR,
            pool_factory=lambda *args, **kwargs: pools.pop(0),
            transport_factory=MagicMock(),
            profile_refresher=lambda *args: SimpleNamespace(
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                universe_sha256=CANARY_UNIVERSE.sha256,
                readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED")),
            ),
        )

    references_pool.closeall.assert_called_once_with()
    control_pool.closeall.assert_called_once_with()
