from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE
from options.domain import AssetType
from options.strategies.domain import CandidateStatus, StructureType
from options.strategy_orchestration import OptionStrategyPipeline
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY
from options.stock_behavior_shadow import OptionStockBehaviorShadowService
from options.stock_behavior_shadow_launch import OptionStockBehaviorShadowLaunch


class FakeAnalysisRepository:
    def __init__(self):
        self.cycle_call = None

    def list_latest_complete_cycle(self, underlyers, context, policy_sha256):
        self.cycle_call = (underlyers, context, policy_sha256)
        return (
            SimpleNamespace(underlyer="AAPL"),
            SimpleNamespace(underlyer="SPY"),
        )

    def get_latest(self, underlyer, context):
        raise AssertionError("full-universe replay must use one coherent cycle")


def test_full_universe_strategy_replay_uses_one_complete_cycle():
    analysis_repository = FakeAnalysisRepository()
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.configuration = SimpleNamespace(
        policy_sha256="a" * 64,
        settings=SimpleNamespace(
            underlyers=("AAPL", "SPY"),
            fixed_etf_underlyers=("SPY",),
        ),
    )
    pipeline.analysis_repository = analysis_repository
    processed = []
    pipeline.process_persisted = lambda run, asset_type: processed.append(
        (run.underlyer, asset_type.value)
    ) or run.underlyer

    assert pipeline.run_latest() == ("AAPL", "SPY")
    assert analysis_repository.cycle_call[0] == ("AAPL", "SPY")
    assert analysis_repository.cycle_call[2] == "a" * 64
    assert processed == [("AAPL", "STOCK"), ("SPY", "ETF")]


class FakeAssessmentRepository:
    def __init__(self, lineage=None, usage=None):
        self.assessments = ()
        self.lineage = lineage
        self.usage = usage or SimpleNamespace(
            assessment_count=0, payload_bytes=0, unavailable_count=0,
            p95_decision_lag_seconds=None,
        )

    def get_matrix_lineage(self, matrix_id):
        assert self.lineage is None or self.lineage.matrix_id == matrix_id
        return self.lineage

    def launch_usage(self, *_args):
        return self.usage

    def persist(self, assessments):
        self.assessments = tuple(assessments)
        return SimpleNamespace(inserted=len(self.assessments), existing=0)


def shadow_launch(**updates):
    values = dict(
        schema_version="option_stock_behavior_shadow_launch_v1",
        launch_id="options-stock-behavior-test-v1",
        starts_at=datetime(2026, 9, 18, 19, 0, tzinfo=timezone.utc),
        ends_at=datetime(2026, 9, 18, 21, 0, tzinfo=timezone.utc),
        underlyers=("AAPL",), maximum_assessments=1000,
        maximum_payload_bytes=50_000_000, maximum_candidates_per_matrix=100,
        minimum_assessments_before_rate_stops=25,
        maximum_unavailable_fraction=0.5,
        maximum_p95_decision_lag_seconds=120,
        artifact_destination="backups/options-stock-behavior/test/status.json",
        detector_policy_sha256=STOCK_BEHAVIOR_GATE_POLICY.sha256,
        behavior_definition_sha256=DEFINITION_SHA256,
        behavior_policy_sha256=OPTIONS_SWING_PROFILE.sha256,
        assessment_only=True, execution_permission=False,
    )
    values.update(updates)
    return OptionStockBehaviorShadowLaunch(**values)


def test_stock_behavior_shadow_persists_selected_directional_unavailable_evidence():
    decision_at = datetime(2026, 9, 18, 20, 5, tzinfo=timezone.utc)
    matrix_id = uuid4()
    context = SimpleNamespace(
        matrix_id=matrix_id, underlyer="AAPL",
        market_data_time=datetime(2026, 9, 18, 20, 0, tzinfo=timezone.utc),
        observed_time=decision_at,
    )
    directional = SimpleNamespace(
        candidate_id=uuid4(), identity_sha256="a" * 64, matrix_id=matrix_id,
        underlyer="AAPL", observed_time=decision_at,
        market_data_time=context.market_data_time,
        strategy_name="DIRECTIONAL_LONG_PREMIUM", strategy_version="strategy_v1",
        structure_type=StructureType.LONG_CALL, status=CandidateStatus.SELECTED,
        primary_evidence={"directional_thesis": "BULLISH"}, policy_sha256="b" * 64,
    )
    unrelated = SimpleNamespace(
        candidate_id=uuid4(), identity_sha256="c" * 64, matrix_id=matrix_id,
        underlyer="AAPL", observed_time=decision_at,
        market_data_time=context.market_data_time,
        strategy_name="CASH_SECURED_PUT", strategy_version="strategy_v1",
        structure_type=StructureType.CASH_SECURED_PUT, status=CandidateStatus.SELECTED,
        primary_evidence={}, policy_sha256="b" * 64,
    )
    assessment_repository = FakeAssessmentRepository(SimpleNamespace(
        matrix_id=matrix_id, underlying="AAPL",
        market_time=context.market_data_time, observed_time=decision_at,
        scheduled_cycle=context.market_data_time - timedelta(minutes=1),
        configuration_sha256="d" * 64,
        market_policy_sha256="e" * 64,
        analysis_policy_sha256="e" * 64,
    ))
    service = OptionStockBehaviorShadowService(
        reference_repository=SimpleNamespace(get_security_as_of=lambda *_: None),
        evidence_repository=SimpleNamespace(
            get_behavior_as_of=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("behavior read cannot occur without a security identity")
            )
        ),
        assessment_repository=assessment_repository,
        launch=shadow_launch(),
    )

    result = service.assess_and_persist(context, (directional, unrelated))

    assert (result.attempted, result.inserted, result.unavailable) == (1, 1, 1)
    assert len(assessment_repository.assessments) == 1
    assessment = assessment_repository.assessments[0]
    assert assessment.candidate_id == directional.candidate_id
    assert assessment.disposition == "UNAVAILABLE"
    assert assessment.option_configuration_sha256 == "d" * 64
    assert assessment.option_market_policy_sha256 == "e" * 64
    assert assessment.option_analysis_policy_sha256 == "e" * 64
    assert assessment.execution_permission is False


@pytest.mark.parametrize(("usage", "expected_status"), [
    (SimpleNamespace(assessment_count=1000, payload_bytes=0, unavailable_count=0,
                     p95_decision_lag_seconds=None), "ASSESSMENT_LIMIT_REACHED"),
    (SimpleNamespace(assessment_count=1, payload_bytes=50_000_000, unavailable_count=0,
                     p95_decision_lag_seconds=None), "PAYLOAD_LIMIT_REACHED"),
    (SimpleNamespace(assessment_count=25, payload_bytes=1000, unavailable_count=13,
                     p95_decision_lag_seconds=1), "UNAVAILABLE_RATE_STOP"),
    (SimpleNamespace(assessment_count=25, payload_bytes=1000, unavailable_count=0,
                     p95_decision_lag_seconds=121), "DECISION_LAG_STOP"),
])
def test_stock_behavior_shadow_stops_before_evidence_read(usage, expected_status):
    decision_at = datetime(2026, 9, 18, 20, 5, tzinfo=timezone.utc)
    matrix_id = uuid4()
    context = SimpleNamespace(
        matrix_id=matrix_id, underlyer="AAPL",
        market_data_time=decision_at - timedelta(minutes=5), observed_time=decision_at,
    )
    candidate = SimpleNamespace(
        candidate_id=uuid4(), identity_sha256="a" * 64, matrix_id=matrix_id,
        underlyer="AAPL", observed_time=decision_at,
        market_data_time=context.market_data_time,
        strategy_name="DIRECTIONAL_LONG_PREMIUM", strategy_version="strategy_v1",
        structure_type=StructureType.LONG_CALL, status=CandidateStatus.SELECTED,
        primary_evidence={"directional_thesis": "BULLISH"}, policy_sha256="b" * 64,
    )
    repository = FakeAssessmentRepository(SimpleNamespace(
        matrix_id=matrix_id, underlying="AAPL", market_time=context.market_data_time,
        observed_time=decision_at, scheduled_cycle=decision_at - timedelta(minutes=6),
        configuration_sha256="c" * 64, market_policy_sha256="d" * 64,
        analysis_policy_sha256="d" * 64,
    ), usage)
    service = OptionStockBehaviorShadowService(
        reference_repository=SimpleNamespace(
            get_security_as_of=lambda *_: (_ for _ in ()).throw(
                AssertionError("stop checks must precede evidence reads")
            )
        ),
        evidence_repository=SimpleNamespace(), assessment_repository=repository,
        launch=shadow_launch(),
    )

    result = service.assess_and_persist(context, (candidate,))

    assert result.status == expected_status
    assert repository.assessments == ()


def test_strategy_pipeline_contains_stock_behavior_shadow_failure():
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.stock_behavior_shadow_service = SimpleNamespace(
        assess_and_persist=lambda *_: (_ for _ in ()).throw(RuntimeError("shadow failed"))
    )

    result, error = pipeline._persist_stock_behavior_shadow(object(), ())

    assert result is None
    assert error == "RuntimeError: shadow failed"


def test_strategy_pipeline_shadow_is_disabled_without_service():
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.stock_behavior_shadow_service = None

    assert pipeline._persist_stock_behavior_shadow(object(), ()) == (None, None)


def test_strategy_pipeline_contains_package_assessment_failure():
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.package_assessment_service = SimpleNamespace(
        assess_and_persist=lambda *_: (_ for _ in ()).throw(
            RuntimeError("package failed")
        )
    )

    result, error = pipeline._persist_package_assessments(())

    assert result is None
    assert error == "RuntimeError: package failed"


def test_auxiliary_assessment_failures_do_not_retry_original_strategy_work():
    matrix_id = uuid4()
    completed = []
    persisted = []
    def fail_after_completion(*_args):
        assert completed, "original strategy work must be acknowledged before optional evidence"
        raise RuntimeError("shadow failed")
    work_repository = SimpleNamespace(
        enqueue=lambda *_args, **_kwargs: None,
        claim_by_business_key=lambda *_args, **_kwargs: SimpleNamespace(work_id=uuid4()),
        complete=lambda work_id, owner: completed.append((work_id, owner)) or True,
        retry=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("shadow failure cannot retry original strategy work")
        ),
    )
    candidate = SimpleNamespace(
        execution_eligibility=None, status=CandidateStatus.SELECTED,
    )
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.configuration = SimpleNamespace(
        policy=SimpleNamespace(capacity=SimpleNamespace(maximum_work_attempts=3)),
        settings=SimpleNamespace(
            event_calendar_provider=None, event_calendar_max_age_seconds=43200,
            equity_context_enabled=False, start_read_only=True,
        ),
        strategy_policy=SimpleNamespace(strategy_version="strategy_v1",
            flow=SimpleNamespace(minimum_print_notional=Decimal("50000"))),
        strategy_policy_sha256="a" * 64,
    )
    pipeline.engine = SimpleNamespace(
        strategy_version="strategy_v1",
        scan=lambda *_args, **_kwargs: SimpleNamespace(
            candidates=(candidate,), scenarios=(),
        ),
    )
    pipeline.work_repository = work_repository
    context = SimpleNamespace()
    pipeline.context_repository = SimpleNamespace(build=lambda *_args, **_kwargs: context)
    trade_reads = []
    pipeline.trade_repository = SimpleNamespace(list_for_contracts=lambda *args, **kwargs:
        trade_reads.append((args, kwargs)) or ())
    pipeline.strategy_repository = SimpleNamespace(
        persist=lambda persisted_context, result: persisted.append(
            (persisted_context, result)
        )
    )
    pipeline.stock_behavior_shadow_service = SimpleNamespace(
        assess_and_persist=fail_after_completion,
    )
    pipeline.package_assessment_service = SimpleNamespace(
        assess_and_persist=lambda *_: (_ for _ in ()).throw(RuntimeError("package failed"))
    )
    decision_context = SimpleNamespace(
        market_time=datetime(2026, 9, 18, 20, 0, tzinfo=timezone.utc),
    )

    result = pipeline.process_matrix(
        matrix_id, "AAPL", decision_context, object(), (), (), AssetType.STOCK,
    )

    assert result.status == "COMPLETE"
    assert result.stock_behavior_shadow_status == "FAILED"
    assert result.stock_behavior_assessment_error == "RuntimeError: shadow failed"
    assert result.package_assessment_status == "FAILED"
    assert result.package_assessment_error == "RuntimeError: package failed"
    assert len(persisted) == 1
    assert len(completed) == 1
    assert trade_reads == [(((), decision_context.market_time - timedelta(hours=8), decision_context),
        {"minimum_notional": Decimal("50000")})]