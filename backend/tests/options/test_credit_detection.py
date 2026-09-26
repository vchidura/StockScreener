from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from options.credit_detection import O3CreditObservation, assess_o3_credit
from options.domain import ContractType
from options.strategies.domain import OptionSide, StructureRiskClass, StructureType
from test_outcome_contracts import package_candidate
from test_strategy_payoff import leg


NOW = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)
EXPIRATION = date(2026, 10, 8)


def credit_fixture(direction: int):
    contract_type = ContractType.PUT if direction == 1 else ContractType.CALL
    structure = StructureType.PUT_CREDIT_VERTICAL if direction == 1 else StructureType.CALL_CREDIT_VERTICAL
    short_strike, long_strike = ((95, 90) if direction == 1 else (105, 110))
    ordered = ((long_strike, "1", OptionSide.BUY), (short_strike, "3", OptionSide.SELL)) if direction == 1 else (
        (short_strike, "3", OptionSide.SELL), (long_strike, "1", OptionSide.BUY))
    legs = tuple(replace(leg(index, str(strike), mark, side, contract_type),
        expiration_date=EXPIRATION, source_market_time=NOW) for index, (strike, mark, side) in enumerate(ordered))
    candidate = replace(package_candidate(legs, structure), strategy_name="SPREAD_RANGE_LOCATOR",
        underlyer="AAPL", structure_risk_class=StructureRiskClass.DEFINED_RISK_CREDIT,
        market_data_time=NOW, observed_time=NOW + timedelta(seconds=1), valid_until=NOW + timedelta(minutes=15),
        primary_evidence={"wall_center": str(short_strike), "wall_strength": 4.0, "wall_open_interest": 5000})
    snapshots = tuple(SimpleNamespace(snapshot_id=item.snapshot_id, open_interest=1000, day_volume=100,
        bid=None, ask=None) for item in legs)
    decision = SimpleNamespace(detector_id="O1", origin="OPTIONS_FIRST", disposition="CONFIRMED", direction=direction,
        decision_at=NOW + timedelta(seconds=2), valid_until=NOW + timedelta(minutes=10),
        activity=SimpleNamespace(episode_id=uuid4()), sha256="d" * 64)
    invalidation = Decimal("96") if direction == 1 else Decimal("104")
    return candidate, decision, snapshots, invalidation


@pytest.mark.parametrize("direction", [1, -1])
def test_o3_credit_maps_direction_and_preserves_model_mark_basis(direction):
    from options.credit_detection import O3_CREDIT_POLICY

    candidate, decision, snapshots, invalidation = credit_fixture(direction)
    observation = assess_o3_credit(candidate, decision,
        activity_contract_id=candidate.legs[0].contract_id, snapshots=snapshots,
        structural_invalidation=invalidation, scheduled_cycle=NOW)
    assert observation.structure_type == ("PUT_CREDIT_VERTICAL" if direction == 1 else "CALL_CREDIT_VERTICAL")
    assert observation.net_credit == observation.maximum_profit == Decimal("200")
    assert observation.maximum_loss == Decimal("300")
    assert observation.pricing_basis == "ORIGINAL_COHERENT_MODEL_MARKS"
    assert observation.policy_version == "o3_options_first_credit_v2"
    assert O3_CREDIT_POLICY["maximum_source_age_seconds"] == 1800
    assert O3_CREDIT_POLICY["pricing_basis"] == observation.pricing_basis
    assert "quote_requirement" not in O3_CREDIT_POLICY
    assert not observation.publication_permission and not observation.execution_permission
    assert O3CreditObservation.model_validate_json(observation.canonical_json()) == observation


@pytest.mark.parametrize("mutation", ["direction", "inside_stop", "activity", "volume", "dte"])
def test_o3_credit_rejects_invalid_or_unqualified_inputs(mutation):
    candidate, decision, snapshots, invalidation = credit_fixture(1)
    activity_contract_id = candidate.legs[0].contract_id
    if mutation == "direction": decision.direction = -1
    if mutation == "inside_stop": invalidation = Decimal("94")
    if mutation == "activity": activity_contract_id = 999999
    if mutation == "volume": snapshots = (SimpleNamespace(**{**vars(snapshots[0]), "day_volume": 49}), snapshots[1])
    if mutation == "dte":
        legs = tuple(replace(item, expiration_date=NOW.date() + timedelta(days=2)) for item in candidate.legs)
        candidate = replace(candidate, legs=legs, expiration_date=legs[0].expiration_date)
    with pytest.raises(ValueError):
        assess_o3_credit(candidate, decision, activity_contract_id=activity_contract_id,
            snapshots=snapshots, structural_invalidation=invalidation, scheduled_cycle=NOW)


def test_o3_collector_admits_indicative_alert_within_shared_budget():
    from options.analytics.alert_selection import DUAL_ORIGIN_SELECTOR_SHA256, O3CreditEvaluationEvidence
    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs

    candidate, decision, snapshots, invalidation = credit_fixture(1)
    observation = assess_o3_credit(candidate, decision,
        activity_contract_id=candidate.legs[0].contract_id, snapshots=snapshots,
        structural_invalidation=invalidation, scheduled_cycle=NOW)
    matrix = dict(underlying="AAPL", matrix_id=candidate.matrix_id,
        market_time=NOW, observed_time=NOW + timedelta(seconds=1))
    source_reader = lambda **_: DetectorCycleInputs(matrices=(matrix,), o3_observations=(observation,))
    repository = SimpleNamespace(prior_selected=lambda **_: {}, prior_o1_observed=lambda **_: set(),
        prior_o3_observed=lambda **_: set(), prior_stock_setup_observed=lambda **_: set())
    configuration = SimpleNamespace(configuration_sha256="c" * 64, strategy_policy_sha256="d" * 64,
        policy_sha256="e" * 64, settings=SimpleNamespace(underlyers=("AAPL",)),
        strategy_policy=SimpleNamespace(strategy_version="phase2_v8_o3_credit_timing", forward_admission=object()))
    run, records = DetectorCycleCollector(source_reader, repository,
        clock=lambda: NOW + timedelta(seconds=5))(configuration=configuration,
            dataset_id="o3-indicative-fixture", scheduled_cycle=NOW,
            completed_matrices={"AAPL": candidate.matrix_id}, started_at=NOW)
    assert len(records) == 1 and isinstance(records[0], O3CreditEvaluationEvidence)
    assert records[0].selection_status == "SELECTED" and records[0].detector_id == "O3"
    assert records[0].selection_reason == "O3_INDICATIVE_ADMISSION"
    assert dict(run.selection_counts) == {"NOT_SELECTED": 0, "OBSERVATION": 0, "REPEAT": 0, "SELECTED": 1}
    assert run.selector_sha256 == DUAL_ORIGIN_SELECTOR_SHA256


def test_o3_eod_review_exposes_credit_economics_without_alert_fields():
    from options.analytics.alert_selection import build_o3_credit_evidence
    from options.analytics.behavior_review import build_detector_evaluation_review

    candidate, decision, snapshots, invalidation = credit_fixture(-1)
    observation = assess_o3_credit(candidate, decision,
        activity_contract_id=candidate.legs[0].contract_id, snapshots=snapshots,
        structural_invalidation=invalidation, scheduled_cycle=NOW)
    record = build_o3_credit_evidence((observation,), dataset_id="o3-eod-fixture",
        selected_at=decision.decision_at)[0]
    repository = SimpleNamespace(review_inputs=lambda **_: dict(ready=True, datasets=(record.dataset_id,),
        dataset_id=record.dataset_id, sessions=(NOW.date(),), session_date=NOW.date(), records=(record,), runs=()))
    result = build_detector_evaluation_review(as_of=decision.decision_at,
        detector="O3", repository=repository)
    row = result["rows"][0]
    assert row["detector_id"] == "O3" and row["selection_status"] == "SELECTED"
    assert row["selection_reason"] == "O3_INDICATIVE_ADMISSION"
    assert row["entry_limit"] is None and row["net_return"] is None
    assert row["observation"]["structure_type"] == "CALL_CREDIT_VERTICAL"
    assert result["models"][2]["selected"] == 1 and result["models"][2]["observations"] == 0


def test_o3_selected_result_moves_from_latest_run_to_day_history():
    from options.analytics.alert_selection import build_o3_credit_evidence
    from options.analytics.behavior_review import build_detector_alert_review

    candidate, decision, snapshots, invalidation = credit_fixture(1)
    first_observation = assess_o3_credit(candidate, decision,
        activity_contract_id=candidate.legs[0].contract_id, snapshots=snapshots,
        structural_invalidation=invalidation, scheduled_cycle=NOW)
    later_cycle = NOW + timedelta(minutes=15)
    later_observation = first_observation.model_copy(update={
        "scheduled_cycle": later_cycle,
        "decision_at": later_cycle + timedelta(seconds=2),
        "valid_until": later_cycle + timedelta(minutes=10),
    })
    dataset = "o3-run-view-fixture"
    first = build_o3_credit_evidence((first_observation,), dataset_id=dataset,
        selected_at=first_observation.decision_at)[0]
    later = build_o3_credit_evidence((later_observation,), dataset_id=dataset,
        selected_at=later_observation.decision_at)[0]
    def run(record):
        return SimpleNamespace(run_id=record.run_id, scheduled_cycle=record.scheduled_cycle,
            selected_at=record.selected_at, market_time=record.scheduled_cycle,
            observed_time=record.selected_at, expected_underlyers=("AAPL",),
            source_matrices=(("AAPL", record.matrix_id),),
            selection_counts=(("SELECTED", 1), ("REPEAT", 0), ("NOT_SELECTED", 0), ("OBSERVATION", 0)),
            rejections=())
    runs = (run(first), run(later))
    repository = SimpleNamespace(completed_runs=lambda **_: runs,
        completed_run=lambda **kwargs: (runs[1], (later,)) if kwargs["scheduled_cycle"] == later.scheduled_cycle else (runs[0], (first,)),
        review_inputs=lambda **_: dict(runs=runs, records=(first, later)))
    latest = build_detector_alert_review(dataset_id=dataset, as_of=later.selected_at,
        detector="O3", repository=repository)
    assert latest["new_alerts"] == 1 and latest["total"] == 1
    assert latest["rows"][0]["detector_id"] == "O3"
    assert latest["rows"][0]["original_package"]["net_premium"] == "200"
    assert latest["rows"][0]["entry_limit"] == "200"
    assert latest["rows"][0]["management_policy"]["stop_loss_multiple"] == "2.0"
    assert latest["rows"][0]["management_policy"]["take_profit_fraction"] == "0.5"
    for leg in latest["rows"][0]["original_package"]["legs"]:
        assert leg["spot"] == "100"
        assert leg["model_mark"] in {"1", "3"}
        assert leg["local_iv"] is not None and leg["local_delta"] is not None
        assert leg["day_volume"] == 100 and leg["open_interest"] == 1000
        assert leg["source_market_time"] and leg["mark_source"]
    history = build_detector_alert_review(dataset_id=dataset, as_of=later.selected_at,
        detector="O3", scope="HISTORY", repository=repository)
    assert history["total"] == 1 and history["rows"][0]["run_id"] == str(first.run_id)
    assert history["latest_run"]["run_id"] == str(later.run_id)


def test_o3_day_history_retains_prior_dataset_after_prospective_cutover():
    from options.analytics.alert_selection import build_o3_credit_evidence
    from options.analytics.behavior_review import build_detector_alert_review

    candidate, decision, snapshots, invalidation = credit_fixture(1)
    first_observation = assess_o3_credit(candidate, decision,
        activity_contract_id=candidate.legs[0].contract_id, snapshots=snapshots,
        structural_invalidation=invalidation, scheduled_cycle=NOW)
    later_cycle = NOW + timedelta(minutes=15)
    later_observation = first_observation.model_copy(update={
        "scheduled_cycle": later_cycle,
        "decision_at": later_cycle + timedelta(seconds=2),
        "valid_until": later_cycle + timedelta(minutes=10),
    })
    prior_dataset = "o3-prior-v26"
    current_dataset = "o3-current-v27"
    first = build_o3_credit_evidence((first_observation,), dataset_id=prior_dataset,
        selected_at=first_observation.decision_at)[0]
    later = build_o3_credit_evidence((later_observation,), dataset_id=prior_dataset,
        selected_at=later_observation.decision_at)[0]
    def run(record):
        return SimpleNamespace(dataset_id=record.dataset_id, run_id=record.run_id,
            scheduled_cycle=record.scheduled_cycle, selected_at=record.selected_at,
            market_time=record.scheduled_cycle, observed_time=record.selected_at,
            expected_underlyers=("AAPL",), source_matrices=(("AAPL", record.matrix_id),),
            selection_counts=(("SELECTED", 1), ("REPEAT", 0), ("NOT_SELECTED", 0), ("OBSERVATION", 0)),
            rejections=())
    prior_runs = (run(first), run(later))
    repository = SimpleNamespace(
        completed_runs=lambda **kwargs: prior_runs if kwargs["dataset_id"] == prior_dataset else (),
        review_inputs=lambda **kwargs: dict(runs=prior_runs, records=(first, later))
            if kwargs["dataset_id"] == prior_dataset else dict(runs=(), records=()),
    )
    history = build_detector_alert_review(dataset_id=current_dataset,
        history_dataset_ids=(prior_dataset, current_dataset), session_date=NOW.date(),
        as_of=later.selected_at, detector="O3", scope="HISTORY", repository=repository)
    assert history["dataset_id"] == current_dataset
    assert history["dataset_ids"] == [prior_dataset, current_dataset]
    assert history["total"] == 1 and history["rows"][0]["run_id"] == str(first.run_id)
    assert history["latest_run"]["run_id"] == str(later.run_id)