from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from options.dual_origin import ActivitySource, detect_option_participation


NOW = datetime(2026, 9, 18, 15, 20, tzinfo=timezone.utc)
MARKET = NOW - timedelta(minutes=20)


def activity_source(**changes):
    values = dict(security_id=uuid4(), underlyer="AAPL", contract_id=123,
        contract_ticker="O:AAPL261016C00100000", contract_type="CALL",
        expiration_date=date(2026, 10, 16), expiration_cutoff=NOW + timedelta(days=28),
        snapshot_id=uuid4(), snapshot_payload_sha256="a" * 64, batch_id=uuid4(), matrix_id=uuid4(),
        configuration_sha256="b" * 64, market_policy_sha256="c" * 64,
        volume_session=MARKET.date(), day_volume=600, open_interest=200,
        oi_settlement_session=date(2026, 9, 17), oi_observed_at=NOW - timedelta(minutes=3),
        market_time=MARKET, observed_at=NOW - timedelta(minutes=3),
        recorded_at=NOW - timedelta(minutes=2), received_at=NOW - timedelta(minutes=1))
    values.update(changes)
    return ActivitySource(**values)


def test_activity_detects_without_direction_and_round_trips():
    source = activity_source()
    result = detect_option_participation(source, market_cutoff=MARKET, decision_at=NOW)
    assert result.disposition == "DETECTED" and result.volume_oi_ratio == 3
    assert result.direction is None
    assert not result.publication_permission and not result.execution_permission
    assert type(result).model_validate_json(result.canonical_json()) == result
    assert source.sha256 == result.source_sha256


def surface_inputs(*, distortion=True):
    from dataclasses import replace
    from math import log
    from equity.behavior import BehaviorComponent, BehaviorMetric, METRICS_V1, StockBehaviorSnapshot
    from options.surface_detection import bind_surface_source

    inputs, _ = package_handoff_inputs()
    snapshot = inputs["snapshots"][0]
    snapshots = []
    for index in range(13):
        strike = 92 + 2 * index
        moneyness = log(strike / float(snapshot.spot))
        iv = .28 + .35 * moneyness ** 2 - .15 * moneyness + (.02 if distortion and strike in (100, 102) else 0)
        snapshots.append(replace(snapshot, contract_id=100 + index, snapshot_id=uuid4(),
            contract_ticker=f"O:FIXTURE{index}", strike=Decimal(strike), local_iv=iv))
    source = bind_surface_source(snapshots=snapshots, lineage=inputs["lineage"], security=inputs["security"],
        recorded_at=inputs["decision_at"], received_at=inputs["decision_at"])
    stock = inputs["stock"]
    component = BehaviorComponent(factor="VOLATILITY", interval="1d", status="READY",
        sources=stock.components[0].sources,
        metrics=tuple(BehaviorMetric(definition=next(metric for metric in METRICS_V1 if metric.metric_id == name),
            interval="1d", status="READY", value=value, sample_count=200)
            for name, value in (("rv20_cc_annual", .25), ("atr14_fraction", .02))))
    stock = StockBehaviorSnapshot.model_validate({**stock.model_dump(), "components": (*stock.components, component)})
    return source, stock, inputs


def test_o2_reuses_surface_fit_and_remains_nondirectional_observation():
    from options.analytics.smile import SmileInput, fit_smile_groups, qualifying_distortions
    from options.domain import ContractType
    from options.surface_detection import SurfaceDecision, assess_surface_first

    source, stock, inputs = surface_inputs()
    result = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"],
        context_until=inputs["planned_exit_at"])
    fit = fit_smile_groups(tuple(SmileInput(point.contract_id, ContractType.CALL, source.expiration_date,
        point.strike, source.spot, point.local_iv) for point in source.points), minimum_strikes=7)[0]
    assert {row.contract_id for row in result.findings} == {row.contract_id for row in qualifying_distortions(fit, minimum_absolute_robust_z=2.5)}
    assert result.finding_disposition == "DETECTED" and result.stock_context_status == "READY"
    assert result.event_context_status == "UNAVAILABLE" and result.direction is None
    assert result.category == "NEUTRAL_VOL" and result.package_status == "NOT_APPLICABLE"
    assert not result.publication_permission and not result.execution_permission
    assert SurfaceDecision.model_validate_json(result.canonical_json()) == result


@pytest.mark.parametrize("mutation", ["smooth", "short", "late", "expired", "missing_stock", "wrong_stock"])
def test_o2_distinguishes_nondetection_bad_source_and_missing_context(mutation):
    from options.surface_detection import SurfaceSource, assess_surface_first

    source, stock, inputs = surface_inputs(distortion=mutation != "smooth")
    decision_at = inputs["decision_at"]
    if mutation == "short": source = SurfaceSource.model_validate({**source.model_dump(), "points": source.points[:6]})
    if mutation == "late": source = source.model_copy(update={"received_at": decision_at + timedelta(seconds=1)})
    if mutation == "expired": decision_at += timedelta(minutes=30)
    if mutation == "missing_stock": stock = None
    if mutation == "wrong_stock": source = source.model_copy(update={"security_id": uuid4()})
    result = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=decision_at)
    if mutation in ("smooth", "short"):
        assert result.finding_disposition == "NOT_DETECTED" and not result.findings
    elif mutation in ("late", "expired"):
        assert result.finding_disposition == "UNAVAILABLE" and not result.findings
    else:
        assert result.finding_disposition == "DETECTED" and result.stock_context_status == "UNAVAILABLE"
    assert result.direction is None and not result.execution_permission


@pytest.mark.parametrize("mutation", ["batch", "expiry", "spot", "revision", "late", "duplicate"])
def test_o2_source_binder_rejects_noncoherent_inputs(mutation):
    from dataclasses import replace
    from options.surface_detection import bind_surface_source

    _, _, inputs = surface_inputs()
    first = inputs["snapshots"][0]
    second = replace(first, snapshot_id=uuid4(), contract_id=first.contract_id + 1,
        contract_ticker="O:OTHER", strike=first.strike + 5)
    if mutation == "batch": second = replace(second, batch_id=uuid4())
    if mutation == "expiry": second = replace(second, expiration_date=second.expiration_date + timedelta(days=1))
    if mutation == "spot": second = replace(second, spot=second.spot + 1)
    if mutation == "revision": second = replace(second, revised_observed_at=inputs["decision_at"])
    if mutation == "late": second = replace(second, first_observed_at=inputs["decision_at"] + timedelta(seconds=1))
    if mutation == "duplicate": second = first
    with pytest.raises(ValueError):
        bind_surface_source(snapshots=(first, second), lineage=inputs["lineage"], security=inputs["security"],
            recorded_at=inputs["decision_at"], received_at=inputs["decision_at"])


def test_o2_known_event_is_context_not_trade_confirmation():
    from options.surface_detection import assess_surface_first

    source, stock, inputs = surface_inputs()
    clock = inputs["decision_at"]
    detail = dict(event_coverage_evidence=[{"source": "fixture"}], holding_event_evidence=[dict(
        source="fixture", source_key="earnings", event_type="EARNINGS", affected_underlying="AAPL",
        first_observed_at=clock - timedelta(minutes=1), scheduled_time=clock + timedelta(minutes=30),
        market_event_id=uuid4(), status="SCHEDULED", confidence="CONFIRMED")])
    result = assess_surface_first(source, stock, decision_at=clock, market_cutoff=clock,
        context_until=inputs["planned_exit_at"], event_detail=detail)
    assert result.finding_disposition == "DETECTED" and result.event_context_status == "BLOCKED"
    assert "EVENT_CONTEXT_BLOCKED" in result.reasons
    assert result.package_status == "NOT_APPLICABLE" and result.probability is None


def test_o2_causal_source_retains_distinct_trade_clocks_and_oldest_expiry():
    from dataclasses import replace
    from options.outcomes import configured_valuation_policy
    from options.surface_detection import CausalSurfaceSource, assess_surface_first, bind_surface_source

    source, stock, inputs = surface_inputs()
    original = inputs["snapshots"][0]
    snapshots = tuple(replace(original, contract_id=point.contract_id, contract_ticker=point.contract_ticker,
        snapshot_id=point.snapshot_id, strike=point.strike, local_iv=point.local_iv,
        market_data_time=original.market_data_time + timedelta(seconds=index + 1),
        mark_market_data_time=original.market_data_time + timedelta(seconds=index + 1))
        for index, point in enumerate(source.points))
    lineage = replace(inputs["lineage"], market_time=max(row.market_data_time for row in snapshots))
    policy = configured_valuation_policy()
    bound = bind_surface_source(snapshots=snapshots, lineage=lineage, security=inputs["security"],
        recorded_at=inputs["decision_at"], received_at=inputs["decision_at"], valuation_policy=policy)
    assert bound.market_time > bound.scheduled_cycle
    assert bound.spot_market_time == original.spot_market_data_time
    assert tuple(point.market_time for point in bound.points) == tuple(row.market_data_time for row in snapshots)
    assert CausalSurfaceSource.model_validate_json(bound.canonical_json()) == bound
    result = assess_surface_first(bound, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    assert result.finding_disposition == "DETECTED"
    assert result.valid_until == snapshots[0].market_data_time + timedelta(seconds=min(1800, policy.maximum_source_age_seconds))
    expired = assess_surface_first(bound, stock, market_cutoff=result.valid_until, decision_at=result.valid_until)
    assert expired.finding_disposition == "UNAVAILABLE" and not expired.findings
    for changes in ({"spot_market_time": bound.market_time + timedelta(seconds=1)},
            {"spot_market_time": bound.spot_market_time - timedelta(seconds=policy.maximum_option_spot_skew_seconds)},
            {"valuation_policy_sha256": "0" * 64}, {"market_time": bound.market_time - timedelta(seconds=1)}):
        with pytest.raises(ValueError):
            CausalSurfaceSource.model_validate({**bound.model_dump(), **changes})


@pytest.mark.parametrize("boundary", ["same", "limit", "future", "over_limit"])
def test_o2_snapshot_clock_policy_boundary(boundary):
    from dataclasses import replace
    from options.outcomes import configured_valuation_policy
    from options.surface_detection import surface_snapshot_eligible

    _, _, inputs = surface_inputs()
    original = inputs["snapshots"][0]
    policy = configured_valuation_policy()
    skew = {"same": 0, "limit": policy.maximum_option_spot_skew_seconds,
        "future": -1, "over_limit": policy.maximum_option_spot_skew_seconds + 1}[boundary]
    snapshot = replace(original, spot_market_data_time=original.market_data_time - timedelta(seconds=skew))
    assert surface_snapshot_eligible(snapshot, valuation_policy=policy) is (boundary in ("same", "limit"))


@pytest.mark.parametrize("mutation", ["future_spot", "skew", "policy", "quality", "revision", "mark_source", "late", "spot", "batch", "expiry"])
def test_o2_causal_binder_keeps_source_quality_and_identity_guards(mutation):
    from dataclasses import replace
    from options.domain import DataQualityFlag, MarkSource
    from options.outcomes import configured_valuation_policy
    from options.surface_detection import bind_surface_source

    _, _, inputs = surface_inputs()
    policy = configured_valuation_policy()
    first = inputs["snapshots"][0]
    second = replace(first, contract_id=999, snapshot_id=uuid4(), strike=first.strike + 1)
    if mutation == "future_spot": second = replace(second, spot_market_data_time=first.market_data_time + timedelta(seconds=1))
    if mutation == "skew": second = replace(second, spot_market_data_time=first.market_data_time - timedelta(seconds=policy.maximum_option_spot_skew_seconds + 1))
    if mutation == "policy": second = replace(second, valuation_policy_sha256="0" * 64)
    if mutation == "quality": second = replace(second, quality_flags=(DataQualityFlag.FALLBACK_MARK,))
    if mutation == "revision": second = replace(second, revised_observed_at=inputs["decision_at"])
    if mutation == "mark_source": second = replace(second, mark_source=MarkSource.DISPLAY_DAY_CLOSE)
    if mutation == "late": second = replace(second, first_observed_at=inputs["decision_at"] + timedelta(seconds=1))
    if mutation == "spot": second = replace(second, spot=first.spot + 1)
    if mutation == "batch": second = replace(second, batch_id=uuid4())
    if mutation == "expiry": second = replace(second, expiration_date=first.expiration_date + timedelta(days=1))
    with pytest.raises(ValueError):
        bind_surface_source(snapshots=(first, second), lineage=inputs["lineage"], security=inputs["security"],
            recorded_at=inputs["decision_at"], received_at=inputs["decision_at"], valuation_policy=policy)


@pytest.mark.parametrize(("changes", "reason"), [
    ({"open_interest": 0}, "POSITIVE_OPEN_INTEREST_REQUIRED"),
    ({"open_interest": None}, "POSITIVE_OPEN_INTEREST_REQUIRED"),
    ({"day_volume": None}, "DAY_VOLUME_UNAVAILABLE"),
    ({"oi_settlement_session": None}, "DATED_OPEN_INTEREST_UNAVAILABLE"),
    ({"oi_settlement_session": date(2026, 9, 16)}, "DATED_OPEN_INTEREST_UNAVAILABLE"),
    ({"oi_observed_at": NOW}, "OPEN_INTEREST_AFTER_SNAPSHOT"),
    ({"received_at": NOW + timedelta(seconds=1)}, "ACTIVITY_NOT_AVAILABLE_AT_DECISION"),
])
def test_activity_fails_closed_on_missing_or_late_evidence(changes, reason):
    source = activity_source(**{**changes, "received_at": max(changes.get("received_at", NOW), NOW)})
    result = detect_option_participation(source, market_cutoff=MARKET, decision_at=NOW)
    assert result.disposition == "UNAVAILABLE" and reason in result.reasons
    assert result.volume_oi_ratio is None


def test_activity_zero_volume_is_measured_and_expiry_boundary_is_strict():
    source = activity_source(day_volume=0)
    result = detect_option_participation(source, market_cutoff=MARKET, decision_at=NOW)
    assert result.disposition == "NOT_DETECTED" and result.volume_oi_ratio == 0
    expired = detect_option_participation(source, market_cutoff=MARKET, decision_at=MARKET + timedelta(minutes=30))
    assert expired.disposition == "UNAVAILABLE" and "ACTIVITY_EXPIRED" in expired.reasons


def test_cumulative_volume_and_new_snapshots_do_not_create_new_episode():
    source = activity_source()
    result = detect_option_participation(source, market_cutoff=MARKET, decision_at=NOW)
    next_source = ActivitySource.model_validate({**source.model_dump(), "day_volume": 900, "snapshot_id": uuid4()})
    repeated = detect_option_participation(next_source, market_cutoff=MARKET, decision_at=NOW)
    assert repeated.episode_id == result.episode_id and repeated.source_sha256 != result.source_sha256
    other = ActivitySource.model_validate({**source.model_dump(), "contract_id": 124})
    assert detect_option_participation(other, market_cutoff=MARKET, decision_at=NOW).episode_id != result.episode_id


@pytest.mark.parametrize("changes", [{"open_interest": -1}, {"day_volume": 1.5}, {"contract_id": True},
    {"recorded_at": NOW + timedelta(days=1)}, {"market_time": MARKET.replace(tzinfo=None)},
    {"volume_session": date(2026, 9, 17)}])
def test_activity_rejects_invalid_source_contract(changes):
    with pytest.raises(ValueError):
        activity_source(**changes)


def confirmation_inputs():
    from options.calendar import OptionExchangeCalendar
    from test_stock_setup_binding import binding_inputs

    inputs = binding_inputs()
    clock = inputs["decision_at"]
    market = clock - timedelta(minutes=5)
    source = activity_source(security_id=inputs["security"].security_id, volume_session=market.date(),
        market_time=market, observed_at=clock - timedelta(minutes=2), recorded_at=clock - timedelta(minutes=1),
        received_at=clock, oi_observed_at=clock - timedelta(minutes=2),
        oi_settlement_session=OptionExchangeCalendar().previous_session(market.date()), expiration_date=date(2026, 10, 16),
        expiration_cutoff=clock + timedelta(days=28))
    return inputs, source


@pytest.mark.parametrize(("mutation", "expected"), [
    (None, "CONFIRMED"), ("missing", "UNAVAILABLE"), ("identity", "UNAVAILABLE"),
    ("opposite", "CONTRADICTED"), ("put", "UNMATCHED"), ("zero_oi", "UNAVAILABLE"),
    ("below", "NOT_DETECTED"), ("future_cutoff", "UNAVAILABLE"), ("expired_stock", "UNAVAILABLE"),
])
def test_o1_confirms_stock_alignment_without_package_or_publication(mutation, expected):
    from options.dual_origin import assess_options_first

    inputs, source = confirmation_inputs()
    stock = inputs["stock"]
    if mutation == "missing": stock = None
    if mutation == "identity": source = source.model_copy(update={"security_id": uuid4()})
    if mutation == "put": source = source.model_copy(update={"contract_type": "PUT"})
    if mutation == "zero_oi": source = source.model_copy(update={"open_interest": 0})
    if mutation == "below": source = source.model_copy(update={"day_volume": 20})
    cutoff = inputs["decision_at"] if mutation != "future_cutoff" else source.market_time - timedelta(seconds=1)
    result = assess_options_first(source, stock, direction=-1 if mutation == "opposite" else 1,
        market_cutoff=cutoff, decision_at=stock.valid_until if mutation == "expired_stock" else inputs["decision_at"])
    assert result.disposition == expected, result.reasons
    if mutation == "expired_stock":
        assert "STOCK_NOT_READY_AT_DECISION" in result.reasons
    assert result.origin_id == str(result.activity.episode_id)
    assert result.package_status == "NOT_ASSESSED" and not result.execution_permission
    assert type(result).model_validate_json(result.canonical_json()) == result


@pytest.mark.parametrize(("mutation", "expected"), [
    (None, "CONFIRMED"), ("missing", "UNAVAILABLE"), ("identity", "UNAVAILABLE"),
    ("put", "UNMATCHED"), ("below", "UNMATCHED"), ("late", "UNAVAILABLE"),
])
def test_s1_retains_exact_stock_episode_and_non_directional_activity_confirmation(mutation, expected):
    from options.dual_origin import StockFirstPolicy, assess_stock_first

    inputs, source = confirmation_inputs()
    setup = inputs["setup"]
    if mutation == "missing": source = None
    if mutation == "identity": source = source.model_copy(update={"security_id": uuid4()})
    if mutation == "put": source = source.model_copy(update={"contract_type": "PUT"})
    if mutation == "below": source = source.model_copy(update={"day_volume": 20})
    decision = setup.source.valid_until if mutation == "late" else inputs["decision_at"]
    result = assess_stock_first(setup, source, policy=StockFirstPolicy(source_policy_sha256=setup.source_policy.sha256),
        trusted_source_policy=setup.source_policy, market_cutoff=decision, decision_at=decision)
    assert result.disposition == expected, result.reasons
    assert result.origin_id == setup.candidate.episode_id and result.direction == setup.candidate.direction
    assert result.origin_detector_version == "range_breakout_acceptance_intraday_v2"
    assert result.confirmation_basis == "MATCHED_CONTRACT_PARTICIPATION_NOT_AGGRESSOR_DIRECTION"
    assert not result.publication_permission and result.package_status == "NOT_ASSESSED"


@pytest.mark.parametrize("model", ["S1", "S2"])
def test_stock_first_indicator_observation_retains_mixed_shadow_metrics(model):
    from datetime import timedelta
    from research.stock_idea_engine import candidate_record
    from options.stock_setup_binding import (
        STOCK_SETUP_INDICATOR_METRICS, StockSetupIndicatorObservation,
        build_stock_setup_indicator_observation, build_stock_setup_shadow_source,
    )

    inputs = qualification_inputs(model=model)
    setup = inputs["setup"]
    decision_at = inputs["decision"].decision_at
    source = build_stock_setup_shadow_source(candidate_payload=candidate_record(setup.candidate),
        episode_id=setup.candidate.episode_id, policy=setup.source_policy,
        publication_payload_sha256=setup.source.payload_sha256,
        publication_market_time=setup.source.market_time, published_at=setup.source.observed_at,
        received_at=decision_at, setup_selection="SELECTED", setup_reason=None)
    observation = build_stock_setup_indicator_observation(source, (inputs["source"],),
        scheduled_cycle=inputs["lineage"].scheduled_cycle, matrix_id=inputs["candidate"].matrix_id,
        decision_at=decision_at)

    assert observation.detector_id == model and observation.baseline_disposition == "CONFIRMED"
    assert tuple(row.metric_id for row in observation.measurements) == STOCK_SETUP_INDICATOR_METRICS
    assert next(row for row in observation.measurements if row.metric_id == "option_matched_contract_count").value == 1
    model_metric = "stock_breakout_clearance_atr" if model == "S1" else "stock_resumption_target_distance_atr"
    other_metric = "stock_resumption_target_distance_atr" if model == "S1" else "stock_breakout_clearance_atr"
    assert next(row for row in observation.measurements if row.metric_id == model_metric).status == "READY"
    assert next(row for row in observation.measurements if row.metric_id == other_metric).status == "UNAVAILABLE"
    assert {row.challenger_id for row in observation.challengers} >= {
        "BASELINE_CURRENT", "OPTION_VOLUME_OI_5_0", "OPTION_BREADTH_2",
        "S1_MIXED_QUALITY" if model == "S1" else "S2_MIXED_QUALITY",
    }
    assert not observation.policy.changes_admission and not observation.execution_permission
    assert StockSetupIndicatorObservation.model_validate_json(observation.canonical_json()) == observation

    expired_at = setup.candidate.expires_at + timedelta(seconds=1)
    expired_source = build_stock_setup_shadow_source(candidate_payload=candidate_record(setup.candidate),
        episode_id=setup.candidate.episode_id, policy=setup.source_policy,
        publication_payload_sha256=setup.source.payload_sha256,
        publication_market_time=setup.source.market_time, published_at=setup.source.observed_at,
        received_at=expired_at, setup_selection="SELECTED", setup_reason=None)
    expired = build_stock_setup_indicator_observation(expired_source, (inputs["source"],),
        scheduled_cycle=inputs["lineage"].scheduled_cycle, matrix_id=inputs["candidate"].matrix_id,
        decision_at=expired_at)
    assert expired.baseline_disposition == "UNAVAILABLE"
    assert expired.baseline_reasons == ("STOCK_EPISODE_EXPIRED_BEFORE_OPTION_DECISION",)
    blocked_source = source.model_copy(update={"setup_selection": "SUPPRESSED", "setup_reason": "INSUFFICIENT_TARGET_ROOM"})
    blocked = build_stock_setup_indicator_observation(blocked_source, (inputs["source"],),
        scheduled_cycle=inputs["lineage"].scheduled_cycle, matrix_id=inputs["candidate"].matrix_id,
        decision_at=decision_at)
    assert blocked.baseline_disposition == "UNAVAILABLE"
    assert blocked.baseline_reasons == ("STOCK_EPISODE_STRUCTURALLY_BLOCKED",)
    from options.analytics.alert_selection import build_stock_setup_indicator_evidence
    from scripts.report_stock_behavior_coverage import summarize_stock_setup_indicator_observations

    records = (*build_stock_setup_indicator_evidence((observation,),
        dataset_id=f"{model.lower()}-mixed-shadow-active", selected_at=decision_at),
        *build_stock_setup_indicator_evidence((expired,),
        dataset_id=f"{model.lower()}-mixed-shadow-expired", selected_at=expired_at))
    report = summarize_stock_setup_indicator_observations(records,
        start_date=observation.scheduled_cycle.date(), end_date=observation.scheduled_cycle.date(),
        as_of=expired_at)
    assert report["status"] == "SHADOW_EVIDENCE_ONLY" and report["observations"] == 2
    assert report["models"][model]["baseline_dispositions"] == {"CONFIRMED": 1, "UNAVAILABLE": 1}
    assert report["models"][model]["expired_before_option_decision"] == 1
    assert report["inference"]["timing_loss_is_separate_cohort"]
    from types import SimpleNamespace
    from options.analytics.behavior_review import build_detector_alert_review, build_detector_evaluation_review

    active_record = records[0]
    review = build_detector_evaluation_review(as_of=active_record.selected_at,
        dataset_id=active_record.dataset_id, detector=model, repository=SimpleNamespace(
            review_inputs=lambda **_: dict(ready=True, datasets=(active_record.dataset_id,),
                dataset_id=active_record.dataset_id, sessions=(observation.scheduled_cycle.date(),),
                session_date=observation.scheduled_cycle.date(), records=(active_record,), runs=())))
    assert review["total"] == 1 and review["rows"][0]["candidate_id"] is None
    assert review["rows"][0]["origin"] == "STOCK_FIRST"
    assert review["rows"][0]["selection_reason"] == "MIXED_INDICATOR_SHADOW_ONLY"
    assert review["models"][[row["detector_id"] for row in review["models"]].index(model)]["observations"] == 1


def intraday_confirmation_inputs():
    from equity.behavior import BehaviorComponent
    from options.intraday_participation import IntradayStockEvidence

    inputs, source = confirmation_inputs()
    market = datetime(2026, 9, 18, 14, 45, tzinfo=timezone.utc)
    decision = market + timedelta(minutes=16)
    source = source.model_copy(update=dict(market_time=market, volume_session=market.date(),
        oi_settlement_session=date(2026, 9, 17), oi_observed_at=market + timedelta(minutes=15),
        observed_at=market + timedelta(minutes=15), recorded_at=decision, received_at=decision,
        expiration_cutoff=datetime(2026, 10, 16, 20, tzinfo=timezone.utc)))
    components = []
    for component in inputs["stock"].components:
        if component.key not in {"TREND.30m", "PARTICIPATION.1d"}:
            continue
        payload = component.model_dump()
        payload["state"] = None
        payload["metrics"] = [metric.model_dump() for metric in component.metrics
                              if metric.definition.metric_id in {"ema50_slope10_atr", "median_dollar_volume20"}]
        for origin in payload["sources"]:
            origin.update(market_time=market - timedelta(minutes=15), observed_at=market,
                recorded_at=market, received_at=market, valid_until=market + timedelta(minutes=30),
                published_at=None)
        components.append(BehaviorComponent.model_validate(payload))
    stock = IntradayStockEvidence(security_id=source.security_id, underlyer=source.underlyer,
        available_at=market, components=tuple(components))
    return source, stock, decision


@pytest.mark.parametrize(("mutation", "expected"), [
    (None, "CONFIRMED"), ("missing_trend", "UNAVAILABLE"), ("missing_liquidity", "UNAVAILABLE"),
    ("future_bar", "UNAVAILABLE"), ("late_receipt", "UNAVAILABLE"), ("expired", "UNAVAILABLE"),
    ("opposite", "CONTRADICTED"), ("put", "UNMATCHED"), ("zero_oi", "UNAVAILABLE"),
    ("identity", "UNAVAILABLE"), ("prior_session", "CONFIRMED"),
    ("prior_session_after_window", "CONFIRMED"), ("older_session", "UNAVAILABLE"),
])
def test_o1_v3_uses_latest_completed_bar_without_other_trends(mutation, expected):
    from options.calendar import OptionExchangeCalendar
    from options.intraday_participation import IntradayStockEvidence, assess_intraday_participation

    source, stock, decision = intraday_confirmation_inputs()
    payload = stock.model_dump()
    if mutation == "missing_trend": payload["components"] = payload["components"][1:]
    if mutation == "missing_liquidity": payload["components"] = payload["components"][:1]
    if mutation == "expired":
        payload["components"][1]["sources"][0]["valid_until"] = decision
    if mutation in {"future_bar", "prior_session", "prior_session_after_window", "older_session"}:
        origin = payload["components"][0]["sources"][0]
        if mutation == "future_bar":
            origin.update(market_time=source.market_time + timedelta(minutes=15), observed_at=decision,
                recorded_at=decision, received_at=decision)
            payload["available_at"] = decision
        if mutation in {"prior_session", "prior_session_after_window", "older_session"}:
            if mutation != "prior_session_after_window":
                option_market = datetime(2026, 9, 18, 13, 45, tzinfo=timezone.utc)
                decision = option_market + timedelta(minutes=16)
                source = source.model_copy(update=dict(market_time=option_market,
                    oi_observed_at=option_market + timedelta(minutes=15),
                    observed_at=option_market + timedelta(minutes=15),
                    recorded_at=decision, received_at=decision))
            prior_session = OptionExchangeCalendar().previous_session(source.volume_session)
            prior_close = OptionExchangeCalendar().session_close(prior_session)
            if mutation == "older_session":
                prior_close = OptionExchangeCalendar().session_close(
                    OptionExchangeCalendar().previous_session(prior_session))
            origin.update(market_time=prior_close, observed_at=prior_close,
                recorded_at=prior_close, received_at=prior_close,
                valid_until=prior_close + timedelta(minutes=30))
            liquidity = payload["components"][1]["sources"][0]
            daily_close = OptionExchangeCalendar().session_close(prior_session)
            liquidity.update(market_time=daily_close, observed_at=daily_close,
                recorded_at=daily_close, received_at=daily_close,
                valid_until=decision + timedelta(hours=1))
            payload["available_at"] = decision
    if mutation == "late_receipt": payload["available_at"] = decision + timedelta(seconds=1)
    if mutation == "identity": payload["underlyer"] = "MSFT"
    if mutation == "put": source = source.model_copy(update={"contract_type": "PUT"})
    if mutation == "zero_oi": source = source.model_copy(update={"open_interest": 0})
    stock = IntradayStockEvidence.model_validate(payload)
    result = assess_intraday_participation(source, stock, direction=-1 if mutation == "opposite" else 1,
        market_cutoff=decision, decision_at=decision)
    assert result.disposition == expected, result.reasons
    assert result.market_cutoff == source.market_time
    assert result.policy.version == "option_participation_stock_alignment_v3"
    assert result.package_status == "NOT_ASSESSED" and not result.execution_permission
    assert type(result).model_validate_json(result.canonical_json()) == result
    if mutation is None:
        assert {gate.gate_id for gate in result.gates} == {"TREND_SLOPE_30m", "UNDERLYING_LIQUIDITY_EVIDENCE"}
        assert result.gates[0].source_market_times == (source.market_time - timedelta(minutes=15),)


def test_o1_v2_rejects_forming_bars_reconstructed_receipts_and_forged_confirmation():
    from options.intraday_participation import IntradayStockEvidence, assess_intraday_participation

    source, stock, decision = intraday_confirmation_inputs()
    for mutation in ("forming", "reconstructed", "no_lineage", "hidden_timeframe"):
        payload = stock.model_dump()
        origin = payload["components"][0]["sources"][0]
        if mutation == "forming": origin["completed_bars_only"] = False
        if mutation == "reconstructed":
            origin.update(availability_mode="RECONSTRUCTED", history_mode="RECONSTRUCTED_HISTORY",
                history_available_at=origin["received_at"])
        if mutation == "no_lineage": payload["components"][0]["sources"] = ()
        if mutation == "hidden_timeframe": payload["components"][0]["interval"] = "1h"
        with pytest.raises(ValueError):
            IntradayStockEvidence.model_validate(payload)
    result = assess_intraday_participation(source, stock, direction=1, market_cutoff=decision, decision_at=decision)
    with pytest.raises(ValueError, match="exact timely stock gates"):
        type(result).model_validate({**result.model_dump(), "gates": ()})


def test_o1_v3_component_adapter_does_not_require_other_trends_or_extend_daily_expiry():
    from equity.behavior import BehaviorComponent, StockBehaviorSnapshot
    from equity.behavior_sources import ADJUSTED_DAILY_HISTORY_POLICY, BEHAVIOR_SOURCE_SELECTION_POLICY
    from options.detector_launch import aligned_stock_windows
    from options.intraday_participation import bind_intraday_components, assess_intraday_participation

    inputs, source = confirmation_inputs()
    payload = inputs["stock"].model_dump()
    completed_window = aligned_stock_windows({source.underlyer: inputs["decision_at"]})[source.underlyer][1]
    assert completed_window is not None
    for component in payload["components"]:
        if component["factor"] == "TREND" and component["interval"] != "30m":
            component.update(status="UNAVAILABLE", state=None, reason_codes=("TEST_OTHER_TREND_MISSING",), metrics=())
        for origin in component["sources"]:
            origin["policy_sha256"] = (BEHAVIOR_SOURCE_SELECTION_POLICY if component["interval"] == "30m" else ADJUSTED_DAILY_HISTORY_POLICY).sha256
            if component["interval"] == "30m":
                origin.update(market_time=completed_window, observed_at=completed_window,
                    recorded_at=completed_window, received_at=completed_window)
            if component["interval"] == "1d":
                origin.update(price_basis="PROVIDER_SPLIT_ADJUSTED", source_manifest_sha256="a" * 64)
    stock = StockBehaviorSnapshot.model_validate(payload)
    assert stock.assess_at(inputs["decision_at"]).data_status != "READY"
    projected = bind_intraday_components(stock, received_at=inputs["decision_at"])
    assert {"TREND.30m", "PARTICIPATION.1d"} <= {row.key for row in projected.components}
    assert projected.carrier_snapshot_sha256 == stock.sha256
    assert {metric.definition.metric_id for row in projected.components for metric in row.metrics} >= {
        "ema50_slope10_atr", "adx14", "median_dollar_volume20"}
    source = source.model_copy(update={"market_time": inputs["decision_at"]})
    source = source.model_copy(update={"observed_at": inputs["decision_at"], "recorded_at": inputs["decision_at"]})
    result = assess_intraday_participation(source, projected, direction=1,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    assert result.disposition == "CONFIRMED", result.reasons
    deadline = min(origin.valid_until for row in projected.components if row.interval == "1d" for origin in row.sources)
    expired = assess_intraday_participation(source, projected, direction=1,
        market_cutoff=inputs["decision_at"], decision_at=deadline)
    assert expired.disposition == "UNAVAILABLE"


def test_o1_indicator_observation_retains_shadow_metrics_without_changing_baseline():
    from types import SimpleNamespace
    from equity.behavior import StockBehaviorSnapshot
    from equity.behavior_sources import ADJUSTED_DAILY_HISTORY_POLICY, BEHAVIOR_SOURCE_SELECTION_POLICY
    from options.detector_launch import aligned_stock_windows
    from options.dual_origin import assess_options_intraday
    from options.intraday_participation import (
        O1_INDICATOR_METRICS, O1IndicatorObservation, bind_intraday_components,
        build_o1_indicator_observation,
    )

    inputs, source = confirmation_inputs()
    payload = inputs["stock"].model_dump()
    completed_window = aligned_stock_windows({source.underlyer: inputs["decision_at"]})[source.underlyer][1]
    assert completed_window is not None
    for component in payload["components"]:
        for origin in component["sources"]:
            origin["policy_sha256"] = (BEHAVIOR_SOURCE_SELECTION_POLICY
                if component["interval"] == "30m" else ADJUSTED_DAILY_HISTORY_POLICY).sha256
            if component["interval"] == "30m":
                origin.update(market_time=completed_window, observed_at=completed_window,
                    recorded_at=completed_window, received_at=completed_window)
            if component["interval"] == "1d":
                origin.update(price_basis="PROVIDER_SPLIT_ADJUSTED", source_manifest_sha256="a" * 64)
    stock = StockBehaviorSnapshot.model_validate(payload)
    projected = bind_intraday_components(stock, received_at=inputs["decision_at"])
    source = source.model_copy(update={"market_time": inputs["decision_at"],
        "observed_at": inputs["decision_at"], "recorded_at": inputs["decision_at"]})
    decision = assess_options_intraday(source, projected, direction=1,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    observation = build_o1_indicator_observation(source, decision, projected,
        scheduled_cycle=inputs["decision_at"])

    assert observation.baseline_disposition == decision.disposition == "CONFIRMED"
    assert tuple((row.component_key, row.metric_id) for row in observation.measurements) == O1_INDICATOR_METRICS
    assert {row.challenger_id for row in observation.challengers} >= {
        "BASELINE_V3", "RETURN5_ALIGNMENT", "ADX14_20", "SLOPE_ABS_0_02",
        "EXTENSION_ABS_MAX_2_0", "DAILY_RVOL20_1_25", "SAME_ELAPSED_30M_RVOL_1_25"}
    same_elapsed = next(row for row in observation.challengers if row.challenger_id == "SAME_ELAPSED_30M_RVOL_1_25")
    assert same_elapsed.verdict == "UNAVAILABLE" and same_elapsed.reason_codes == ("UNAVAILABLE_NOT_IMPLEMENTED",)
    assert O1IndicatorObservation.model_validate_json(observation.canonical_json()) == observation
    assert not observation.policy.changes_admission and not observation.publication_permission

    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs
    from options.analytics.behavior_review import build_detector_alert_review, build_detector_evaluation_review

    configuration = detector_run_inputs()["configuration"]
    configuration.settings.underlyers = (source.underlyer,)
    matrices = {source.underlyer: source.matrix_id}
    cycle_inputs = DetectorCycleInputs(matrices=(dict(underlying=source.underlyer,
        matrix_id=source.matrix_id, market_time=source.market_time,
        observed_time=decision.decision_at),), o1_observations=(observation,))
    collector = DetectorCycleCollector(lambda **_: cycle_inputs,
        SimpleNamespace(prior_selected=lambda **_: {}), clock=lambda: decision.decision_at)
    run, records = collector(configuration=configuration, dataset_id="o1-indicator-fixture",
        scheduled_cycle=observation.scheduled_cycle, completed_matrices=matrices,
        started_at=decision.decision_at)
    assert len(records) == 1 and records[0].selection_status == "OBSERVATION"
    assert dict(run.selection_counts) == {"NOT_SELECTED": 0, "OBSERVATION": 1, "REPEAT": 0, "SELECTED": 0}
    review = build_detector_evaluation_review(as_of=decision.decision_at,
        dataset_id="o1-indicator-fixture", detector="O1", repository=SimpleNamespace(
            review_inputs=lambda **_: dict(ready=True, datasets=("o1-indicator-fixture",),
                dataset_id="o1-indicator-fixture", sessions=(observation.scheduled_cycle.date(),),
                session_date=observation.scheduled_cycle.date(), records=records, runs=(run,))))
    assert review["total"] == 1 and review["rows"][0]["candidate_id"] is None
    assert review["rows"][0]["selection_reason"] == "INDICATOR_SHADOW_ONLY"
    assert review["models"][0]["observations"] == 1 and review["models"][0]["selected"] == 0
    alert_repository = SimpleNamespace(completed_runs=lambda **_: (run,), completed_run=lambda **_: (run, records),
        repeat_counts=lambda **kwargs: {} if kwargs["records"] == [] else pytest.fail("observation entered package repeat lookup"))
    latest_review = build_detector_alert_review(dataset_id="o1-indicator-fixture",
        as_of=decision.decision_at, detector="O1", repository=alert_repository)
    assert latest_review["total"] == 0 and latest_review["detected_observations"] == 1
    assert latest_review["rows"] == []
    newer = run.model_copy(update={"scheduled_cycle": run.scheduled_cycle + timedelta(minutes=15),
        "selected_at": run.selected_at + timedelta(minutes=15), "record_sha256s": (),
        "selection_counts": tuple((status, 0) for status, _ in run.selection_counts)})
    alert_repository.completed_runs = lambda **_: (run, newer)
    alert_repository.completed_run = lambda **_: (newer, ())
    alert_repository.review_inputs = lambda **_: dict(runs=(run, newer), records=records)
    assert build_detector_alert_review(dataset_id="o1-indicator-fixture",
        as_of=newer.selected_at, detector="O1", repository=alert_repository)["total"] == 0
    history_review = build_detector_alert_review(dataset_id="o1-indicator-fixture",
        as_of=newer.selected_at, detector="O1", repository=alert_repository, scope="HISTORY")
    assert history_review["total"] == 0 and history_review["detected_observations"] == 1
    assert history_review["rows"] == []
    from scripts.report_stock_behavior_coverage import summarize_o1_indicator_observations
    report = summarize_o1_indicator_observations(records,
        start_date=observation.scheduled_cycle.date(), end_date=observation.scheduled_cycle.date(),
        as_of=decision.decision_at)
    assert report["status"] == "SHADOW_EVIDENCE_ONLY" and report["observations"] == 1
    assert report["baseline_dispositions"] == {"CONFIRMED": 1}
    assert report["distinct_dataset_episodes"] == 1 and report["duplicate_dataset_episode_rows"] == 0
    assert report["metric_availability"]["ema50_slope10_atr"]["ready"] == 1
    assert report["challenger_verdicts"]["SAME_ELAPSED_30M_RVOL_1_25"]["unavailable"] == 1
    assert report["inference"] == dict(independent_samples=False, correlated_observations=True,
        outcome_comparison_ready=False, admission_changed=False, execution_changed=False)


def test_intraday_launch_requires_its_policy_and_expanded_runtime_pins():
    from pathlib import Path
    from options.detector_launch import decode_detector_forward_launch, LatestCompletedDetectorForwardLaunch, INTRADAY_RUNTIME_FILES, _source_hashes
    from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY

    backend = Path(__file__).resolve().parents[2]
    from test_equity_behavior_setup import direct_policy, publication_inputs
    from equity.behavior_setup import DirectResumptionSourcePolicy

    _, _, _, original_policy = publication_inputs()
    acceptance = direct_policy(original_policy)
    resumption = DirectResumptionSourcePolicy(instance_id=acceptance.instance_id,
        instance_policy_sha256=acceptance.instance_policy_sha256, publication_policy_sha256=acceptance.publication_policy_sha256,
        runtime_sources=acceptance.runtime_sources)
    launch = LatestCompletedDetectorForwardLaunch(dataset_id="intraday-launch-fixture", effective_from=NOW,
        underlyers=("AAPL",), configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64,
        valuation_policy_sha256="c" * 64, stock_ledger="backups/fixture.sqlite", acceptance_source=acceptance,
        resumption_source=resumption, runtime_sources=_source_hashes(backend, INTRADAY_RUNTIME_FILES),
        o1_confirmation_policy_sha256=INTRADAY_ALIGNMENT_POLICY.sha256)
    assert decode_detector_forward_launch(launch.canonical_json()) == launch
    assert launch.alert_mode == "DEVELOPMENT_OBSERVATIONS"
    for changes in ({"o1_confirmation_policy_sha256": "0" * 64}, {"runtime_sources": launch.runtime_sources[:-1]}):
        with pytest.raises(ValueError):
            LatestCompletedDetectorForwardLaunch.model_validate({**launch.model_dump(), **changes})


def test_stock_setup_ready_launch_pins_exact_window_wait_policy():
    from pathlib import Path
    from options.detector_launch import (
        INTRADAY_RUNTIME_FILES, STOCK_SETUP_PUBLICATION_WAIT_POLICY,
        StockSetupReadyDetectorForwardLaunch, decode_detector_forward_launch, _source_hashes,
    )
    from test_equity_behavior_setup import direct_policy, publication_inputs
    from equity.behavior_setup import DirectResumptionSourcePolicy
    from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY

    backend = Path(__file__).resolve().parents[2]
    _, _, _, original_policy = publication_inputs()
    acceptance = direct_policy(original_policy)
    resumption = DirectResumptionSourcePolicy(instance_id=acceptance.instance_id,
        instance_policy_sha256=acceptance.instance_policy_sha256,
        publication_policy_sha256=acceptance.publication_policy_sha256,
        runtime_sources=acceptance.runtime_sources)
    launch = StockSetupReadyDetectorForwardLaunch(dataset_id="setup-wait-launch-fixture", effective_from=NOW,
        underlyers=("AAPL",), configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64,
        valuation_policy_sha256="c" * 64, stock_ledger="backups/fixture.sqlite", acceptance_source=acceptance,
        resumption_source=resumption, runtime_sources=_source_hashes(backend, INTRADAY_RUNTIME_FILES),
        o1_confirmation_policy_sha256=INTRADAY_ALIGNMENT_POLICY.sha256)
    assert decode_detector_forward_launch(launch.canonical_json()) == launch
    assert launch.stock_readiness_policy == "PER_MODEL_EXACT_WINDOW_WAIT_V2"
    assert launch.stock_setup_wait_policy_sha256 == STOCK_SETUP_PUBLICATION_WAIT_POLICY.sha256
    with pytest.raises(ValueError):
        StockSetupReadyDetectorForwardLaunch.model_validate({**launch.model_dump(),
            "stock_setup_wait_policy_sha256": "0" * 64})


def test_o1_v3_preserves_package_qualification_and_freezes_confirmation_provenance():
    import json
    from options.detector_launch import aligned_stock_windows
    from options.dual_origin import assess_options_intraday, qualify_dual_origin_package
    from options.intraday_participation import IntradayStockEvidence

    inputs = technical_qualification_inputs()
    original, _ = qualify_dual_origin_package(**inputs)
    stock = inputs["stock"]
    payload = stock.model_dump()
    payload["components"] = [row for row in payload["components"]
        if f'{row["factor"]}.{row["interval"]}' in {"TREND.30m", "PARTICIPATION.1d"}]
    window = aligned_stock_windows({stock.ticker: inputs["source"].market_time})[stock.ticker][1]
    assert window is not None
    trend = next(row for row in payload["components"] if row["interval"] == "30m")
    for origin in trend["sources"]:
        origin.update(market_time=window, observed_at=window, recorded_at=window,
            received_at=window, valid_until=inputs["decision"].decision_at + timedelta(hours=1))
    evidence = IntradayStockEvidence(security_id=stock.security_id, underlyer=stock.ticker,
        available_at=stock.available_at, components=tuple(payload["components"]))
    inputs["stock"] = evidence
    inputs["decision"] = assess_options_intraday(inputs["source"], evidence, direction=1,
        market_cutoff=inputs["decision"].market_cutoff, decision_at=inputs["decision"].decision_at)
    package, plan = qualify_dual_origin_package(**inputs)
    assert package.recurrence_sha256 != original.recurrence_sha256
    assert package.exposure_sha256 == original.exposure_sha256
    payload = json.loads(plan.payload_json)
    assert payload["confirmation_policy_version"] == "option_participation_stock_alignment_v3"
    assert payload["stock_confirmation"] == evidence.model_dump(mode="json")
    assert payload["management_policy"]["technical_exit"]


def test_o1_latest_completed_signal_version_retains_v1_and_exact_gate_policy():
    from options.dual_origin import assess_options_intraday, load_signal_decision, SignalDecision

    source, stock, decision = intraday_confirmation_inputs()
    result = assess_options_intraday(source, stock, direction=1, market_cutoff=decision, decision_at=decision)
    assert result.disposition == "CONFIRMED" and result.schema_version == "dual_origin_signal_decision_v4"
    assert load_signal_decision(result) == result
    with pytest.raises(ValueError):
        SignalDecision.model_validate(result.model_dump())
    for changes in ({"gates": ()}, {"confirmation_policy_sha256": "0" * 64}):
        with pytest.raises(ValueError):
            type(result).model_validate({**result.model_dump(), **changes})


def test_o2_retained_sample_uses_first_run_and_does_not_select_on_followup():
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_surface_evidence
    from options.surface_detection import assess_surface_first
    from scripts.report_stock_behavior_coverage import surface_observation_sample

    source, stock, inputs = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    records = build_surface_evidence((observation,), dataset_id="o2-sample-fixture", selected_at=inputs["decision_at"])
    record = records[0]
    first = SimpleNamespace(dataset_id=record.dataset_id, run_id=record.run_id,
        scheduled_cycle=record.scheduled_cycle, selected_at=record.selected_at)
    subsequent = observation.model_copy(update=dict(scheduled_cycle=observation.scheduled_cycle + timedelta(minutes=15),
        decision_at=observation.decision_at + timedelta(minutes=15), market_cutoff=observation.market_cutoff + timedelta(minutes=15),
        valid_until=observation.valid_until + timedelta(minutes=15), findings=(), finding_disposition="NOT_DETECTED"))
    later_records = build_surface_evidence((subsequent,), dataset_id=record.dataset_id, selected_at=subsequent.decision_at)
    later = SimpleNamespace(dataset_id=record.dataset_id, run_id=later_records[0].run_id,
        scheduled_cycle=subsequent.scheduled_cycle, selected_at=subsequent.decision_at)
    baseline = surface_observation_sample(((first, records),))
    report = surface_observation_sample(((later, later_records), (first, records)))
    assert len(report["selected"]) == 1
    selected = report["selected"][0]
    assert selected["snapshot_id"] == baseline["selected"][0]["snapshot_id"]
    assert selected["followup_observations"][0]["status"] == "NOT_REDETECTED_IN_RETAINED_COHORT"
    assert selected["followup_observations"][0]["residual_iv_points"] is None
    assert selected["outcome_status"] == "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"
    assert not report["publication_permission"] and not report["execution_permission"]
    assert surface_observation_sample(())["selected"] == []
    with pytest.raises(ValueError):
        surface_observation_sample(((first, records),), limit=6)


def test_o2_sample_followup_rejects_changed_membership_clocks_and_measurements():
    import json
    from copy import deepcopy
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_surface_evidence
    from options.surface_detection import assess_surface_first
    from scripts.report_stock_behavior_coverage import surface_observation_sample, validate_surface_sample_seed

    source, stock, inputs = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    records = build_surface_evidence((observation,), dataset_id="o2-followup-fixture", selected_at=inputs["decision_at"])
    run = SimpleNamespace(dataset_id=records[0].dataset_id, run_id=records[0].run_id,
        scheduled_cycle=records[0].scheduled_cycle, selected_at=records[0].selected_at)
    sample = surface_observation_sample(((run, records),))
    sample.update(dataset_id=run.dataset_id, evidence_basis="ORIGINAL_HASH_VERIFIED_COMPLETED_RUNS")
    sample = json.loads(json.dumps(sample, default=str))
    assert len(validate_surface_sample_seed(sample, run, records)) == 1
    for field in ("contract_id", "published_at", "robust_z", "evaluation_sha256"):
        changed = deepcopy(sample)
        changed["selected"][0][field] = "changed"
        with pytest.raises(ValueError, match="changed"):
            validate_surface_sample_seed(changed, run, records)


def test_o2_checkpoint_reader_keeps_causal_bounds_and_does_not_fallback_for_missing_contract():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from scripts.report_stock_behavior_coverage import read_surface_checkpoint_rows

    cursor = MagicMock()
    matrix = dict(matrix_id=uuid4(), batch_id=uuid4(), market_time=MARKET, observed_time=NOW)
    cursor.fetchone.return_value = matrix
    cursor.fetchall.return_value = ()
    run = SimpleNamespace(configuration_sha256="a" * 64, market_policy_sha256="b" * 64, scheduled_cycle=MARKET)
    result = read_surface_checkpoint_rows(cursor, run=run, underlying="SPY", expiration=date(2026, 9, 25),
        contract_type="PUT", checkpoint_at=NOW)
    assert result == (matrix, ())
    assert cursor.execute.call_count == 2
    query, parameters = cursor.execute.call_args.args
    assert "created_at<=%s" in query and "LIMIT 5001" in query
    assert parameters[-2:] == (NOW, NOW)


def surface_checkpoint_inputs():
    from dataclasses import replace
    from math import log
    from options.outcomes import configured_valuation_policy

    inputs, _ = package_handoff_inputs()
    original = inputs["snapshots"][0]
    baseline, followup = [], []
    batch = uuid4()
    for index in range(13):
        strike = 92 + 2 * index
        moneyness = log(strike / float(original.spot))
        smooth = .28 + .35 * moneyness ** 2 - .15 * moneyness
        perturbation = ((index % 3) - 1) * .001
        row = replace(original, contract_id=100 + index, snapshot_id=uuid4(),
            contract_ticker=f"O:FIXTURE{index}", strike=Decimal(strike), local_iv=smooth + perturbation)
        baseline.append(replace(row, local_iv=row.local_iv + (.02 if strike in (100, 102) else 0)))
        followup.append(replace(row, snapshot_id=uuid4(), batch_id=batch,
            market_data_time=row.market_data_time + timedelta(minutes=15),
            mark_market_data_time=row.mark_market_data_time + timedelta(minutes=15),
            spot_market_data_time=row.spot_market_data_time + timedelta(minutes=15),
            first_observed_at=row.first_observed_at + timedelta(minutes=15)))
    return dict(baseline_peers=tuple(baseline), snapshots=tuple(followup), contract_id=104,
        checkpoint_at=original.market_data_time + timedelta(minutes=16), valuation_policy=configured_valuation_policy())


def test_o2_fixed_checkpoint_measures_below_threshold_without_treating_it_as_missing():
    from options.analytics.surface_followup import measure_surface_checkpoint

    result = measure_surface_checkpoint(**surface_checkpoint_inputs())
    assert result["status"] == "MEASURED_FIXED_PEERS"
    assert result["baseline_fit"]["meets_original_distortion_rule"]
    assert not result["fixed_peer_fit"]["meets_original_distortion_rule"]
    assert result["absolute_residual_change_iv_points"] < 0
    assert result["fixed_peer_fit"]["residual_iv_points"] is not None


@pytest.mark.parametrize(("mutation", "reason"), [
    ("absent", "CONTRACT_ABSENT_FROM_LATEST_MATRIX"), ("duplicate", "AMBIGUOUS_CONTRACT_SNAPSHOTS"),
    ("future", "SOURCE_NOT_AVAILABLE_AT_CHECKPOINT"), ("stale", "COHORT_EXPIRED_AT_CHECKPOINT"),
    ("unchanged", "NO_NEW_OPTION_MARK"), ("policy", "VALUATION_BASIS_CHANGED"),
    ("revision", "TARGET_SOURCE_INELIGIBLE"), ("identity", "CONTRACT_IDENTITY_CHANGED"),
    ("missing_peer", "ORIGINAL_PEER_COHORT_INCOMPLETE"),
])
def test_o2_fixed_checkpoint_retains_missingness_and_exact_source_constraints(mutation, reason):
    from dataclasses import replace
    from options.analytics.surface_followup import measure_surface_checkpoint

    inputs = surface_checkpoint_inputs()
    rows = list(inputs["snapshots"])
    if mutation == "absent": rows = [row for row in rows if row.contract_id != inputs["contract_id"]]
    if mutation == "duplicate": rows.append(rows[4])
    if mutation == "future": rows[4] = replace(rows[4], first_observed_at=inputs["checkpoint_at"] + timedelta(seconds=1))
    if mutation == "stale": inputs["checkpoint_at"] += timedelta(minutes=30)
    if mutation == "unchanged": rows = list(inputs["baseline_peers"])
    if mutation == "policy": rows[4] = replace(rows[4], valuation_policy_sha256="0" * 64)
    if mutation == "revision": rows[4] = replace(rows[4], revised_observed_at=inputs["checkpoint_at"])
    if mutation == "identity": rows[4] = replace(rows[4], shares_per_contract=10)
    if mutation == "missing_peer": rows = rows[1:]
    result = measure_surface_checkpoint(**{**inputs, "snapshots": tuple(rows)})
    assert result["reason"] == reason
    assert result["absolute_residual_change_iv_points"] is None
    assert result["fixed_peer_fit"] is None
    if mutation != "missing_peer":
        assert result["available_original_peer_count"] is None
    if mutation == "revision":
        assert result["target_revised_observed_at"] == inputs["checkpoint_at"]


def test_s1_rejects_untrusted_policy_and_o1_rejects_forged_pass():
    from options.dual_origin import SignalDecision, StockFirstPolicy, assess_options_first, assess_stock_first

    inputs, source = confirmation_inputs()
    setup = inputs["setup"]
    with pytest.raises(ValueError, match="trusted"):
        assess_stock_first(setup, source, policy=StockFirstPolicy(source_policy_sha256="0" * 64),
            trusted_source_policy=setup.source_policy, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    result = assess_options_first(source, inputs["stock"], direction=1,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    payload = result.model_dump()
    payload["gates"] = ()
    with pytest.raises(ValueError, match="exact stock gates"):
        SignalDecision.model_validate(payload)


def test_participation_inventory_is_bounded_read_only_and_retains_missingness(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.repositories.daily_facts import OptionDailyFactRepository

    matrix = dict(matrix_id=uuid4(), batch_id=uuid4(), underlying="AAPL", market_time=MARKET,
        observed_time=NOW, completed_at=NOW, scheduled_cycle=MARKET)
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[matrix], []]
    @contextmanager
    def read():
        yield cursor
    repository = OptionDailyFactRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    configuration = SimpleNamespace(settings=SimpleNamespace(underlyers=("AAPL",)),
        configuration_sha256="b" * 64, policy_sha256="c" * 64)
    result = repository.participation_inventory(configuration=configuration, as_of=NOW)
    assert len(result) == 1 and result[0]["counts"] == {}
    assert result[0]["oi_settlement_session"] == date(2026, 9, 17)
    for call in cursor.execute.call_args_list:
        query = call.args[0]
        assert query.strip().startswith(("SET", "SELECT", "WITH"))
        if len(call.args) > 1:
            assert query.count("%s") == len(call.args[1])
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    assert "daily.open_interest_observed_at<=snapshot.first_observed_at" in cursor.execute.call_args.args[0]
    assert "session_close>%s" in cursor.execute.call_args.args[0]
    assert "AND revised_observed_at IS NULL) AS fresh_at_cutoff" in cursor.execute.call_args.args[0]


def test_dual_origin_readiness_cli_forbids_output_before_database(monkeypatch):
    import sys
    from scripts import report_stock_behavior_coverage

    monkeypatch.setattr(sys, "argv", ["report", "--dual-origin-readiness", "--output", "unused.json"])
    with pytest.raises(SystemExit):
        report_stock_behavior_coverage.parse_args()


def test_detector_schema_report_is_read_only_and_does_not_scan_sources(monkeypatch):
    import sys
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from scripts import report_stock_behavior_coverage

    cursor = MagicMock()
    cursor.fetchone.side_effect = [dict(table_present=False), dict(waiting_locks=0)]
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    assert repository.schema_readiness() == dict(table_present=False, waiting_locks=0)
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    assert all(call.args[0].strip().startswith(("SET", "SELECT")) for call in cursor.execute.call_args_list)
    monkeypatch.setattr(sys, "argv", ["report", "--detector-schema", "--output", "forbidden.json"])
    with pytest.raises(SystemExit):
        report_stock_behavior_coverage.parse_args()


def package_handoff_inputs(structure="LONG_CALL"):
    from dataclasses import replace
    from options.calendar import OptionExchangeCalendar
    from options.dual_origin import bind_activity_source
    from options.repositories.daily_facts import DailyOpenInterestRecord
    from test_stock_setup_binding import binding_inputs

    inputs = binding_inputs(structure)
    snapshot = replace(inputs["snapshots"][0], day_volume=600, open_interest=200)
    session = OptionExchangeCalendar().session_for_slot(inputs["lineage"].scheduled_cycle)
    record = DailyOpenInterestRecord(snapshot.contract_id, OptionExchangeCalendar().previous_session(session),
        snapshot.underlyer, 200, snapshot.first_observed_at, session, snapshot.batch_id)
    source = bind_activity_source(snapshot=snapshot, open_interest_record=record, lineage=inputs["lineage"],
        security=inputs["security"], recorded_at=inputs["source_received_at"], received_at=inputs["source_received_at"])
    return inputs, source


@pytest.mark.parametrize("structure", ["LONG_CALL", "LONG_PUT", "CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"])
def test_both_routes_bind_same_package_category_but_keep_detector_attribution(structure):
    from options.dual_origin import StockFirstPolicy, assess_options_first, assess_stock_first, bind_confirmed_candidate

    inputs, source = package_handoff_inputs(structure)
    setup = inputs["setup"]
    direction = setup.candidate.direction
    o1 = assess_options_first(source, inputs["stock"], direction=direction,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    s1 = assess_stock_first(setup, source, policy=StockFirstPolicy(source_policy_sha256=setup.source_policy.sha256),
        trusted_source_policy=setup.source_policy, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    handoffs = [bind_confirmed_candidate(decision, source, inputs["candidate"], inputs["package_assessment"])
        for decision in (o1, s1)]
    assert all(row.primary_category == "MOMENTUM" for row in handoffs)
    assert handoffs[0].exposure_sha256 == handoffs[1].exposure_sha256
    assert handoffs[0].recurrence_sha256 != handoffs[1].recurrence_sha256
    assert all(row.remaining_gates and not row.publication_permission for row in handoffs)


@pytest.mark.parametrize("mutation", ["matrix", "contract", "snapshot", "expiry", "late", "premium", "unconfirmed", "thesis", "recorded_thesis"])
def test_candidate_handoff_rejects_mismatched_contracts_economics_and_deadlines(mutation):
    from dataclasses import replace
    from decimal import Decimal
    from options.dual_origin import assess_options_first, bind_confirmed_candidate

    inputs, source = package_handoff_inputs()
    decision = assess_options_first(source, inputs["stock"], direction=1,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    candidate = inputs["candidate"]
    if mutation == "matrix": candidate = replace(candidate, matrix_id=uuid4())
    if mutation == "contract": candidate = replace(candidate, legs=(replace(candidate.legs[0], contract_id=999),))
    if mutation == "snapshot": candidate = replace(candidate, legs=(replace(candidate.legs[0], snapshot_id=uuid4()),))
    if mutation == "expiry": candidate = replace(candidate, legs=(replace(candidate.legs[0], expiration_date=date(2026, 10, 23)),))
    if mutation == "late": candidate = replace(candidate, valid_until=decision.decision_at)
    if mutation == "premium": candidate = replace(candidate, net_premium=Decimal("-1"))
    if mutation == "unconfirmed": decision = decision.model_copy(update={"disposition": "UNAVAILABLE", "reasons": ("MISSING",)})
    if mutation == "thesis": candidate = replace(candidate, strategy_name="INCOME_WHEEL")
    if mutation == "recorded_thesis": candidate = replace(candidate, primary_evidence={"directional_thesis": "BEARISH"})
    with pytest.raises(ValueError):
        bind_confirmed_candidate(decision, source, candidate, inputs["package_assessment"])


def test_signal_review_deduplicates_refreshes_and_leaves_success_unavailable():
    from options.dual_origin import StockFirstPolicy, assess_options_first, assess_stock_first, bind_confirmed_candidate, summarize_signal_decisions

    inputs, source = package_handoff_inputs()
    setup = inputs["setup"]
    clock = inputs["decision_at"]
    o1 = assess_options_first(source, inputs["stock"], direction=1, market_cutoff=clock, decision_at=clock)
    s1 = assess_stock_first(setup, source, policy=StockFirstPolicy(source_policy_sha256=setup.source_policy.sha256),
        trusted_source_policy=setup.source_policy, market_cutoff=clock, decision_at=clock)
    later = assess_options_first(source, inputs["stock"], direction=1, market_cutoff=clock, decision_at=clock + timedelta(seconds=1))
    handoffs = [bind_confirmed_candidate(decision, source, inputs["candidate"], inputs["package_assessment"])
        for decision in (o1, s1)]
    report = summarize_signal_decisions([o1, o1, s1, later], handoffs + handoffs, as_of=clock)
    assert report["cross_model_shared_exposures"] == 1
    assert all(row["assessments"] == row["origin_episodes"] == row["candidate_references"] == 1 for row in report["models"])
    assert all(row["positive_net_marked_return_rate"] is None and row["measured_outcomes"] is None for row in report["models"])
    assert not report["repeated_assessments_are_hits"]
    with pytest.raises(ValueError, match="exact confirmed parent"):
        summarize_signal_decisions([o1], handoffs, as_of=clock)
    assert summarize_signal_decisions([], as_of=clock)["models"] == []
    below = assess_options_first(source.model_copy(update={"day_volume": 1}), inputs["stock"], direction=1,
        market_cutoff=clock, decision_at=clock)
    assert summarize_signal_decisions([below], as_of=clock)["models"][0]["origin_episodes"] == 0


def test_activity_uses_previous_exchange_session_and_rejects_unresolved_revisions():
    from options.calendar import OptionExchangeCalendar

    market = datetime(2026, 9, 8, 15, tzinfo=timezone.utc)
    clock = market + timedelta(minutes=20)
    source = activity_source(market_time=market, volume_session=market.date(), observed_at=clock - timedelta(minutes=3),
        recorded_at=clock - timedelta(minutes=2), received_at=clock - timedelta(minutes=1),
        oi_observed_at=clock - timedelta(minutes=3), oi_settlement_session=date(2026, 9, 4))
    assert OptionExchangeCalendar().previous_session(market.date()) == date(2026, 9, 4)
    assert detect_option_participation(source, market_cutoff=market, decision_at=clock).disposition == "DETECTED"
    revised = source.model_copy(update={"revised_observed_at": clock - timedelta(minutes=1)})
    result = detect_option_participation(revised, market_cutoff=market, decision_at=clock)
    assert result.disposition == "UNAVAILABLE" and "REVISED_ACTIVITY_SOURCE_REQUIRES_VERSIONED_ADAPTER" in result.reasons


def test_source_binder_does_not_assign_oi_date_to_mismatched_value():
    from dataclasses import replace
    from options.dual_origin import bind_activity_source
    from options.repositories.daily_facts import DailyOpenInterestRecord

    inputs, original = package_handoff_inputs()
    snapshot = replace(inputs["snapshots"][0], day_volume=600, open_interest=200)
    bad_record = DailyOpenInterestRecord(snapshot.contract_id, original.oi_settlement_session,
        snapshot.underlyer, 201, snapshot.first_observed_at, original.volume_session, snapshot.batch_id)
    source = bind_activity_source(snapshot=snapshot, open_interest_record=bad_record, lineage=inputs["lineage"],
        security=inputs["security"], recorded_at=inputs["decision_at"], received_at=inputs["decision_at"])
    assert source.oi_settlement_session is None
    assert detect_option_participation(source, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"]).disposition == "UNAVAILABLE"


def qualified_package(index=1, *, model="O1", underlyer="AAPL", direction=1, rank=1, **changes):
    from uuid import NAMESPACE_URL, UUID, uuid5
    from options.dual_origin import QualifiedDualOriginPackage, QualifiedResumptionPackage

    values = dict(handoff_sha256="a" * 64, decision_sha256="b" * 64, candidate_id=UUID(int=index),
        candidate_identity_sha256=f"{index:064x}", matrix_id=uuid5(NAMESPACE_URL, f"fixture:{underlyer}"),
        scheduled_cycle=MARKET - timedelta(hours=1), detector_id=model,
        underlyer=underlyer, direction=direction, candidate_rank=rank, exposure_sha256=f"{index:064x}",
        recurrence_sha256=f"{index + {'O1': 0, 'S1': 100000, 'S2': 200000}[model]:064x}",
        plan_sha256="c" * 64, source_basis_sha256="d" * 64, decision_at=NOW - timedelta(minutes=1),
        entry_deadline=NOW + timedelta(minutes=1), exit_deadline=NOW + timedelta(hours=1),
        expires_at=NOW + timedelta(days=10), event_horizon_status="UNAVAILABLE")
    values.update(changes)
    contract = QualifiedResumptionPackage if model == "S2" else QualifiedDualOriginPackage
    return contract(**values)


def test_dual_origin_global_cap_and_deterministic_fair_allocation():
    from options.analytics.alert_selection import select_dual_origin_packages

    names = tuple(f"T{index}" for index in range(13))
    packages = [qualified_package(1 + stock * 4 + model_index * 2 + side_index,
        model=model, underlyer=symbol, direction=direction)
        for stock, symbol in enumerate(names) for model_index, model in enumerate(("O1", "S1"))
        for side_index, direction in enumerate((-1, 1))]
    kwargs = dict(decision_at=NOW, scheduled_cycle=packages[0].scheduled_cycle,
        expected_underlyers=names, completed_matrices={row.underlyer: row.matrix_id for row in packages})
    result = select_dual_origin_packages(packages, {}, **kwargs)
    assert len(result["members"]) == 20
    assert result["evidence"]["by_model"]["O1"]["new_alerts"] == 10
    assert result["evidence"]["by_model"]["S1"]["new_alerts"] == 10
    assert result["evidence"]["rejections"]["RUN_CAP"] == 32
    assert len(result["evaluation_rows"]) == 52
    assert result["evidence"]["qualified_not_selected"] == 32
    assert {row["package"].sha256 for row in result["evaluation_rows"]} == {row.sha256 for row in packages}
    assert result == select_dual_origin_packages(list(reversed(packages)), {}, **kwargs)


def test_all_three_package_models_share_one_twenty_alert_budget():
    from options.analytics.alert_selection import select_dual_origin_packages

    packages = [qualified_package(1000 + model_index * 100 + index, model=model, underlyer=f"T{index}")
        for model_index, model in enumerate(("O1", "S1", "S2")) for index in range(13)]
    kwargs = dict(decision_at=NOW, scheduled_cycle=packages[0].scheduled_cycle,
        expected_underlyers=tuple(f"T{index}" for index in range(13)),
        completed_matrices={row.underlyer: row.matrix_id for row in packages})
    result = select_dual_origin_packages(packages, {}, **kwargs)
    assert len(result["members"]) == 20 and len(result["evaluation_rows"]) == 39
    assert sorted(row["new_alerts"] for row in result["evidence"]["by_model"].values()) == [6, 7, 7]
    assert result["evidence"]["qualified_not_selected"] == 19
    assert result == select_dual_origin_packages(list(reversed(packages)), {}, **kwargs)


def test_selector_v2_evidence_stays_readable_and_cannot_claim_s2():
    from options.analytics.alert_selection import DetectorSelectionEvidence, DUAL_ORIGIN_SELECTOR_V2_SHA256

    record = evaluation_records()[0]
    prior = DetectorSelectionEvidence.model_validate({**record.model_dump(), "selector_sha256": DUAL_ORIGIN_SELECTOR_V2_SHA256})
    assert prior.package.detector_id == "O1"
    assert DetectorSelectionEvidence.model_validate_json(prior.canonical_json()) == prior


def test_dual_origin_repeats_do_not_consume_new_lane_or_change_original_terms():
    from options.analytics.alert_selection import select_dual_origin_packages

    first = qualified_package(decision_at=NOW - timedelta(hours=1))
    repeat = qualified_package(2, recurrence_sha256=first.recurrence_sha256,
        exposure_sha256=first.exposure_sha256, expires_at=first.expires_at)
    fresh = qualified_package(3, rank=2)
    result = select_dual_origin_packages([repeat, repeat, fresh], {first.recurrence_sha256: first},
        decision_at=NOW, scheduled_cycle=repeat.scheduled_cycle,
        expected_underlyers=("AAPL",), completed_matrices={"AAPL": repeat.matrix_id})
    assert result["members"] == [fresh]
    assert len(result["observations"]) == 1
    assert result["observations"][0]["first_candidate_id"] == str(first.candidate_id)
    assert first.decision_at == NOW - timedelta(hours=1)
    assert {row["selection_status"] for row in result["evaluation_rows"]} == {"REPEAT", "SELECTED"}
    assert len(result["evaluation_rows"]) == 2


def test_qualified_lane_overflow_is_retained_without_becoming_an_alert_hit():
    from options.analytics.alert_selection import (
        DUAL_ORIGIN_SELECTOR_POLICY, DUAL_ORIGIN_SELECTOR_POLICY_V1, DUAL_ORIGIN_SELECTOR_SHA256,
        DUAL_ORIGIN_SELECTOR_V1_SHA256, select_dual_origin_packages,
    )

    best, overflow = qualified_package(1), qualified_package(2, rank=2)
    result = select_dual_origin_packages([best, overflow], {}, decision_at=NOW,
        scheduled_cycle=best.scheduled_cycle, expected_underlyers=("AAPL",), completed_matrices={"AAPL": best.matrix_id})
    assert result["members"] == [best] and result["observations"] == []
    assert result["evidence"]["qualified_not_selected"] == 1
    record = next(row for row in result["evaluation_rows"] if row["package"] == overflow)
    assert record["selection_status"] == "NOT_SELECTED"
    assert record["selection_reason"] == "LOWER_RANK_SAME_DETECTOR_UNDERLYING_DIRECTION"
    assert DUAL_ORIGIN_SELECTOR_POLICY_V1["maximum_new_alerts"] == 50
    assert DUAL_ORIGIN_SELECTOR_POLICY["maximum_new_alerts"] == 20
    assert DUAL_ORIGIN_SELECTOR_SHA256 != DUAL_ORIGIN_SELECTOR_V1_SHA256


def evaluation_records():
    from options.analytics.alert_selection import build_selection_evidence, select_dual_origin_packages
    from options.dual_origin import qualify_dual_origin_package

    inputs = qualification_inputs()
    package, plan = qualify_dual_origin_package(**inputs)
    selection = select_dual_origin_packages([package], {}, decision_at=package.decision_at,
        scheduled_cycle=package.scheduled_cycle, expected_underlyers=("AAPL",), completed_matrices={"AAPL": package.matrix_id})
    return build_selection_evidence(selection, {plan.sha256: plan}, dataset_id="forward-fixture-v1", selected_at=package.decision_at)


def test_evaluation_evidence_preserves_plan_and_original_selection():
    from options.analytics.alert_selection import DetectorSelectionEvidence

    record = evaluation_records()[0]
    assert record.selection_status == "SELECTED" and record.selection_reason == "WITHIN_RUN_BUDGET"
    assert DetectorSelectionEvidence.model_validate_json(record.canonical_json()) == record
    for changes in ({"selection_status": "NOT_SELECTED"}, {"plan_payload_text": "{}"}, {"run_id": uuid4()}):
        with pytest.raises(ValueError):
            DetectorSelectionEvidence.model_validate({**record.model_dump(), **changes})


def test_evaluation_evidence_accepts_valid_plan_above_legacy_32k_bound():
    import hashlib
    import json
    from options.analytics.alert_selection import DetectorSelectionEvidence
    from options.strategies.domain import canonical_json

    record = evaluation_records()[0]
    plan = json.loads(record.plan_payload_text)
    plan["retained_source_probe"] = ""
    base = canonical_json(plan)
    plan["retained_source_probe"] = "x" * (33000 - len(base))
    plan_payload_text = canonical_json(plan)
    package = type(record.package).model_validate({
        **record.package.model_dump(),
        "plan_sha256": hashlib.sha256(plan_payload_text.encode("ascii")).hexdigest(),
    })

    expanded = DetectorSelectionEvidence.model_validate({
        **record.model_dump(),
        "package": package,
        "plan_payload_text": plan_payload_text,
    })

    assert 32768 < len(plan_payload_text.encode("ascii")) < 65536
    assert len(expanded.canonical_json().encode("ascii")) <= 65536


def detector_run_inputs(records=()):
    from types import SimpleNamespace

    clock = records[0].selected_at if records else NOW
    cycle = records[0].scheduled_cycle if records else MARKET
    matrix_id = records[0].matrix_id if records else uuid4()
    config = SimpleNamespace(configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64,
        policy_sha256="c" * 64, strategy_policy=SimpleNamespace(strategy_version="phase2_v4"),
        settings=SimpleNamespace(underlyers=("AAPL",)))
    return dict(configuration=config, dataset_id=records[0].dataset_id if records else "zero-fixture-v1",
        scheduled_cycle=cycle, selected_at=clock,
        matrices=[dict(underlying="AAPL", matrix_id=matrix_id, market_time=cycle, observed_time=clock)],
        records=records, rejections={})


def test_complete_detector_run_retains_zero_alerts_and_reconciles_members():
    from options.analytics.alert_selection import DetectorRunEvidence, build_detector_run

    empty = build_detector_run(**detector_run_inputs())
    assert empty.record_sha256s == () and dict(empty.selection_counts)["SELECTED"] == 0
    assert DetectorRunEvidence.model_validate_json(empty.canonical_json()) == empty
    records = evaluation_records()
    populated = build_detector_run(**detector_run_inputs(records))
    assert populated.run_id == records[0].run_id and dict(populated.selection_counts)["SELECTED"] == 1
    with pytest.raises(ValueError, match="counts"):
        DetectorRunEvidence.model_validate({**populated.model_dump(), "record_sha256s": ()})
    inputs = detector_run_inputs(records)
    inputs["configuration"].settings.underlyers = ("AAPL", "MSFT")
    with pytest.raises(ValueError, match="complete source matrices"):
        build_detector_run(**inputs)


@pytest.mark.parametrize("model", ["O1", "S1", "S2", "O2", "EMPTY"])
def test_detector_collector_reuses_qualification_and_preserves_complete_zero_run(model):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs

    source = qualification_inputs(model=model if model in ("S1", "S2") else "O1")
    clock = source["decision"].decision_at
    lineage = source["lineage"]
    packages, surfaces = (), ()
    if model == "O2":
        surface, stock, values = surface_inputs()
        lineage = values["lineage"]
        clock = values["decision_at"]
        surfaces = (dict(source=surface, stock=stock, market_cutoff=clock, decision_at=clock),)
    elif model != "EMPTY":
        packages = (source,)
    configuration = detector_run_inputs()["configuration"]
    matrices = {"AAPL": lineage.matrix_id}
    inputs = DetectorCycleInputs(matrices=(dict(underlying="AAPL", matrix_id=lineage.matrix_id,
        market_time=lineage.market_time, observed_time=lineage.observed_time),),
        package_inputs=packages, surface_inputs=surfaces)
    reader = MagicMock(return_value=inputs)
    prior = MagicMock(return_value={})
    collector = DetectorCycleCollector(reader, SimpleNamespace(prior_selected=prior), clock=lambda: clock)
    run, records = collector(configuration=configuration, dataset_id="collector-fixture-v1",
        scheduled_cycle=lineage.scheduled_cycle, completed_matrices=matrices, started_at=clock)
    assert len(records) == (0 if model == "EMPTY" else 1)
    assert dict(run.selection_counts)["SELECTED"] == (1 if model in ("O1", "S1", "S2") else 0)
    assert dict(run.selection_counts)["OBSERVATION"] == (1 if model == "O2" else 0)
    prior.assert_called_once()
    reader.assert_called_once()


def test_forward_run_distinguishes_scheduled_slot_from_actual_source_watermark():
    from options.analytics.alert_selection import build_detector_run, load_detector_run

    inputs = detector_run_inputs()
    inputs["matrices"][0]["market_time"] = inputs["scheduled_cycle"] + timedelta(minutes=5)
    with pytest.raises(ValueError, match="causal"):
        build_detector_run(**inputs)
    inputs["configuration"].strategy_policy.forward_admission = True
    run = build_detector_run(**inputs)
    assert run.schema_version == "option_detector_run_v2"
    assert run.market_time > run.scheduled_cycle and run.market_time <= run.observed_time <= run.selected_at
    assert load_detector_run(run.canonical_json()) == run
    inputs["matrices"][0]["market_time"] = run.selected_at + timedelta(seconds=1)
    with pytest.raises(ValueError, match="causal"):
        build_detector_run(**inputs)


def test_zero_run_repository_writes_header_and_never_invents_members(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.analytics.alert_selection import build_detector_run
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    inputs = detector_run_inputs()
    run = build_detector_run(**inputs)
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, None]
    cursor.fetchall.side_effect = [inputs["matrices"], []]
    @contextmanager
    def write():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", write)
    inserts = MagicMock()
    monkeypatch.setattr("options.repositories.alert_evaluations.execute_values", inserts)
    result = repository.persist_completed_run(run, ())
    assert result["status"] == "RECORDED" and result["new_alerts"] == 0
    inserts.assert_not_called()
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("INSERT INTO option_board_publications" in query for query in queries)
    assert not any("option_board_members" in query for query in queries)
    for call in cursor.execute.call_args_list:
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])


@pytest.mark.parametrize("model", ["S1", "S2"])
@pytest.mark.parametrize("stock_window", ["legacy", "ready", "missing"])
def test_production_assembler_qualifies_exact_inputs_and_blocks_known_events(model, stock_window, tmp_path):
    from dataclasses import asdict
    from types import SimpleNamespace
    from options.detector_collection import DetectorCycleCollector, ProductionDetectorSourceReader
    from equity.polygon import sha256_json
    from equity.materialization import SETUP_VERSION

    inputs = qualification_inputs(model=model)
    candidate, source, lineage = inputs["candidate"], inputs["source"], inputs["lineage"]
    clock = inputs["decision"].decision_at
    row = asdict(candidate)
    row["candidate_identity"] = row.pop("identity_sha256")
    row["underlying"] = row.pop("underlyer")
    row["candidate_rank"] = row.pop("rank")
    legs = tuple(dict(leg, candidate_id=candidate.candidate_id) for leg in row.pop("legs"))
    references = []
    for leg, original in zip(candidate.legs, inputs["references"]):
        reference = asdict(original)
        reference.update(contract_id=leg.contract_id, underlying=reference.pop("underlyer"),
            eligibility_status="VALIDATED_ACTIVE", additional_underlyings=[], adjustment_metadata={"cfi": "OCASPS"})
        references.append(reference)
    bars = tuple(dict(asdict(bar), created_at=created) for bar, created in zip(inputs["raw_bars"], inputs["raw_bar_created_ats"]))
    retained = dict(matrices=(dict(underlying=candidate.underlyer, matrix_id=candidate.matrix_id,
        market_time=lineage.market_time, observed_time=lineage.observed_time),), candidates=(dict(candidate_id=candidate.candidate_id),),
        activity=(dict(source=source, snapshot=inputs["snapshots"][0], lineage=lineage,
            finding=detect_option_participation(source, market_cutoff=max(lineage.market_time, lineage.scheduled_cycle), decision_at=clock),
            security=inputs["security"], stock=inputs["stock"]),), surface_inputs=(), rejections={})
    spot = inputs["snapshots"][0].spot
    payload = dict(stops=[dict(source="Price Action", price=float(spot - 1))],
        targets=[dict(source="Price Action", price=float(spot + 3))], technicals=dict(atr=1), strategy_results={})
    structure = dict(source_version=SETUP_VERSION, security_id=inputs["security"].security_id, ticker=candidate.underlyer,
        direction=1, payload=payload, payload_sha256=sha256_json(payload), observed_at=clock, created_at=clock,
        feature_bar_ids=(bars[0]["bar_revision_id"],), interval="1h", market_time=lineage.market_time,
        valid_until=inputs["decision"].valid_until)
    technical = dict(sources=(structure,), bars=bars, events=(), coverage=())
    repository = SimpleNamespace(detector_package_sources=lambda **_: dict(candidates=(row,), legs=legs, references=references, raw_bars=bars),
        detector_technical_sources=lambda **_: technical)
    def setups(**arguments):
        if stock_window == "legacy":
            return (inputs["setup"],)
        assert arguments["market_cutoffs"] == {candidate.underlyer: max(lineage.scheduled_cycle, lineage.market_time)}
        if stock_window == "missing":
            from equity.stock_alert_results import read_direct_stock_setup_windows
            from options.detector_launch import aligned_stock_windows

            return read_direct_stock_setup_windows(tmp_path / "absent.sqlite", policies=(inputs["setup"].source_policy,),
                windows=aligned_stock_windows(arguments["market_cutoffs"]), as_of=arguments["as_of"], clock=lambda: clock)
        return (inputs["setup"],), {}
    reader = ProductionDetectorSourceReader(SimpleNamespace(read=lambda **_: retained), repository,
        SimpleNamespace(latest_covered_actions=lambda *_, **__: ({"coverage": True}, ())),
        setup_reader=setups, clock=lambda: clock, aligned_setup_windows=stock_window != "legacy")
    configuration = detector_run_inputs()["configuration"]
    configuration.valuation_policy = inputs["valuation_policy"]
    configuration.valuation_policy_sha256 = inputs["valuation_policy"].policy_sha256
    configuration.strategy_policy.forward_admission = True
    collector = DetectorCycleCollector(reader, SimpleNamespace(prior_selected=lambda **_: {}), clock=lambda: clock)
    arguments = dict(configuration=configuration, dataset_id="production-assembler-fixture",
        scheduled_cycle=lineage.scheduled_cycle, completed_matrices={candidate.underlyer: candidate.matrix_id}, started_at=clock)
    if stock_window == "ready":
        from research.stock_idea_engine import candidate_record
        from options.stock_setup_binding import build_stock_setup_shadow_source

        setup = inputs["setup"]
        shadow_source = build_stock_setup_shadow_source(candidate_payload=candidate_record(setup.candidate),
            episode_id=setup.candidate.episode_id, policy=setup.source_policy,
            publication_payload_sha256=setup.source.payload_sha256,
            publication_market_time=setup.source.market_time, published_at=setup.source.observed_at,
            received_at=clock, setup_selection="SELECTED", setup_reason=None)
        shadow_calls = []
        def shadow_setups(**arguments):
            shadow_calls.append(arguments)
            return (shadow_source,), {}
        shadow_reader = ProductionDetectorSourceReader(SimpleNamespace(read=lambda **_: retained), repository,
            SimpleNamespace(latest_covered_actions=lambda *_, **__: ({"coverage": True}, ())),
            setup_reader=setups, setup_shadow_reader=shadow_setups,
            clock=lambda: clock, aligned_setup_windows=True, setup_wait_enabled=True)
        shadow_inputs = shadow_reader(**arguments)
        assert len(shadow_inputs.stock_setup_observations) == 1
        assert shadow_inputs.stock_setup_observations[0].detector_id == model
        assert len(shadow_inputs.package_inputs) == 2
        assert shadow_calls[0]["as_of"] == clock
        assert "wait_deadline" not in shadow_calls[0] and "progress_callback" not in shadow_calls[0]
    from scripts.report_stock_behavior_coverage import package_binding_diagnostic
    diagnostic = package_binding_diagnostic(retained, repository.detector_package_sources())
    assert diagnostic["package_reasons"] == {"EXACT_PACKAGE_SOURCES_AVAILABLE": 1}
    unavailable = package_binding_diagnostic(retained, {**repository.detector_package_sources(), "references": (), "raw_bars": ()})
    assert unavailable["package_reasons"] == {"CAUSAL_REFERENCE_UNAVAILABLE": 1, "EXACT_RAW_SPOT_UNAVAILABLE": 1}
    run, records = collector(**arguments)
    expected = {"O1"} if stock_window == "missing" else {"O1", model}
    assert {record.detector_id for record in records} == expected, dict(run.rejections)
    if stock_window == "missing":
        assert dict(run.rejections)["S1_STOCK_SOURCE_UNAVAILABLE"] == 1
        assert dict(run.rejections)["S2_STOCK_SOURCE_UNAVAILABLE"] == 1
        assert not (tmp_path / "absent.sqlite").exists()
        retained["surface_inputs"] = ("unchanged-O2-source",)
        assert reader(**arguments).surface_inputs == retained["surface_inputs"]
        retained["surface_inputs"] = ()
    assert run.schema_version == "option_detector_run_v2"
    technical["events"] = (dict(source="fixture", source_key="event", event_type="EARNINGS",
        affected_underlying=candidate.underlyer, first_observed_at=clock, scheduled_time=clock + timedelta(minutes=30),
        market_event_id=uuid4(), status="SCHEDULED", confidence="CONFIRMED"),)
    technical["coverage"] = ()
    blocked, rows = collector(**arguments)
    assert rows == () and dict(blocked.rejections)["KNOWN_EVENT_IN_HOLDING_WINDOW"] == len(expected)
    technical.update(events=(), sources=(), bars=(), source_error="O1_TECHNICAL_SOURCE_TIMEOUT")
    partial, rows = collector(**arguments)
    assert {record.detector_id for record in rows} == (set() if stock_window == "missing" else {model})
    assert dict(partial.rejections)["O1_TECHNICAL_SOURCE_TIMEOUT"] == 1
    retained["activity"][0]["stock"] = None
    def unconfirmed_sources(**arguments):
        assert arguments["technical_underlyers"] == ()
        return dict(events=(), coverage=(), sources=(), bars=())
    repository.detector_technical_sources = unconfirmed_sources
    collector(**arguments)


def test_production_structural_source_rejects_atr_only_or_adjusted_levels():
    from dataclasses import asdict
    from options.detector_collection import bind_structural_technical_source
    from equity.polygon import sha256_json
    from equity.materialization import SETUP_VERSION

    inputs = qualification_inputs()
    security = inputs["security"]
    raw = asdict(inputs["raw_bars"][0])
    payload = dict(stops=[dict(source="Price Action", price=98)], targets=[dict(source="Price Action", price=104)],
        technicals=dict(atr=2), strategy_results={})
    row = dict(source_version=SETUP_VERSION, security_id=security.security_id, direction=1, payload=payload,
        payload_sha256=sha256_json(payload), observed_at=NOW, created_at=NOW, feature_bar_ids=(raw["bar_revision_id"],),
        interval="1h", market_time=NOW, valid_until=NOW + timedelta(minutes=30))
    bars = {raw["bar_revision_id"]: raw}
    result = bind_structural_technical_source(row, bars, security=security, direction=1, spot=Decimal("100"), received_at=NOW)
    assert result.levels[0].price == 98
    raw["adjusted"] = True
    with pytest.raises(ValueError, match="raw feature history"):
        bind_structural_technical_source(row, bars, security=security, direction=1, spot=Decimal("100"), received_at=NOW)
    raw["adjusted"] = False
    payload["targets"][0]["source"] = "ATR"
    row["payload_sha256"] = sha256_json(payload)
    with pytest.raises(ValueError, match="no ATR-only substitute"):
        bind_structural_technical_source(row, bars, security=security, direction=1, spot=Decimal("100"), received_at=NOW)


def test_retained_candidate_decoder_preserves_original_terms():
    from dataclasses import asdict
    from options.alert_qualification import retained_candidate

    original = qualification_inputs()["candidate"]
    row = asdict(original)
    row["candidate_identity"] = row.pop("identity_sha256")
    row["underlying"] = row.pop("underlyer")
    row["candidate_rank"] = row.pop("rank")
    legs = row.pop("legs")
    assert retained_candidate(row, legs) == original
    row["execution_eligibility"] = "ENABLED"
    with pytest.raises(ValueError, match="research-only"):
        retained_candidate(row, legs)


def test_detector_attempt_reader_requires_explicit_scoped_status(tmp_path):
    import json
    from types import SimpleNamespace
    from options.repositories.alert_review_sources import detector_attempt_status

    launch = SimpleNamespace(dataset_id="fixture", configuration_sha256="a" * 64, effective_from=NOW - timedelta(hours=1))
    path = tmp_path / "backups/options-worker/detector-status.json"
    assert detector_attempt_status(launch=launch, as_of=NOW, backend_dir=tmp_path) == dict(available=False, attempts=[])
    path.parent.mkdir(parents=True)
    cycle = NOW - timedelta(minutes=15)
    row = dict(scheduled_cycle=cycle.isoformat(), started_at=cycle.isoformat(), finished_at=NOW.isoformat(),
        status="FAILED", reason="DETECTOR_SOURCE_TIMEOUT")
    payload = dict(version="option_detector_operational_status_v1", dataset_id="fixture",
        configuration_sha256="a" * 64, attempts=[row])
    path.write_text(json.dumps(payload))
    result = detector_attempt_status(launch=launch, as_of=NOW, backend_dir=tmp_path)
    assert result["available"] and result["attempts"][0]["status"] == "FAILED"
    assert not detector_attempt_status(launch=launch, as_of=NOW, completed=[cycle], backend_dir=tmp_path)["attempts"]
    for changes in ({"dataset_id": "old"}, {"configuration_sha256": "b" * 64}, {"attempts": [row, row]},
            {"attempts": [{**row, "finished_at": (NOW + timedelta(seconds=1)).isoformat()}]}, {"attempts": [row] * 97}):
        path.write_text(json.dumps({**payload, **changes}))
        assert not detector_attempt_status(launch=launch, as_of=NOW, backend_dir=tmp_path)["available"]
    row.update(status="RUNNING", finished_at=None, started_at=(NOW - timedelta(minutes=40)).isoformat(),
        scheduled_cycle=(NOW - timedelta(minutes=45)).isoformat())
    path.write_text(json.dumps(payload))
    assert detector_attempt_status(launch=launch, as_of=NOW, backend_dir=tmp_path)["attempts"][0]["status"] == "UNVERIFIED"


def test_detector_package_source_queries_are_bounded_read_only(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    @contextmanager
    def read():
        yield cursor
    repository = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    assert repository.detector_package_sources(configuration=detector_run_inputs()["configuration"],
        candidate_ids=(), as_of=NOW) == dict(candidates=(), legs=(), references=(), raw_bars=())
    reference_query = next(call for call in cursor.execute.call_args_list if "option_contract_catalog_versions" in call.args[0])
    assert "version.valid_from<=candidate.market_data_time" in reference_query.args[0]
    assert "version.valid_to>candidate.market_data_time" in reference_query.args[0]
    assert "version.first_observed_at)<=candidate.observed_time" in reference_query.args[0]
    assert "JOIN LATERAL" in reference_query.args[0]
    assert "version.catalog_version_id DESC LIMIT 1" in reference_query.args[0]
    assert reference_query.args[1][0] == []
    for call in cursor.execute.call_args_list:
        assert call.args[0].strip().startswith(("SET", "SELECT"))
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])


def test_optional_technical_timeout_preserves_event_read_and_unrelated_models(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from psycopg2.errors import QueryCanceled
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    cursor = MagicMock()
    source = dict(feature_bar_ids=(uuid4(),))
    event = dict(status="SCHEDULED", affected_underlying="AAPL")
    cursor.fetchall.side_effect = [[source], [event], [{"coverage": True}]]
    @contextmanager
    def read():
        yield cursor
    repository = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    monkeypatch.setattr(repository, "_technical_bars", MagicMock(side_effect=QueryCanceled("timeout")))
    result = repository.detector_technical_sources(underlyers=("AAPL", "SPY"), technical_underlyers=("AAPL",), market_cutoff=NOW, as_of=NOW)
    assert result["sources"] == result["bars"] == ()
    assert result["events"] == (event,) and result["coverage"] == ({"coverage": True},)
    assert result["source_error"] == "O1_TECHNICAL_SOURCE_TIMEOUT"
    select_calls = [call for call in cursor.execute.call_args_list if call.args[0].startswith("SELECT")]
    assert select_calls[0].args[1][0] == ["AAPL"]
    assert select_calls[1].args[1][0] == ["AAPL", "SPY"]
    assert any(call.args[0] == "ROLLBACK TO SAVEPOINT detector_optional_structure" for call in cursor.execute.call_args_list)
    repository._technical_bars.reset_mock()
    cursor.fetchall.side_effect = [[], [event], []]
    result = repository.detector_technical_sources(underlyers=("AAPL",), technical_underlyers=(), market_cutoff=NOW, as_of=NOW)
    repository._technical_bars.assert_not_called()
    assert result["events"] == (event,)


def test_technical_bar_lookup_retries_same_snapshot_under_one_budget():
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
    from psycopg2.errors import QueryCanceled

    cursor = MagicMock()
    ids = [uuid4() for _ in range(1001)]
    cursor.fetchall.return_value = [{"bar_start": NOW, "bar_revision_id": ids[0]}]
    attempts = []
    def execute(query, parameters=None):
        if query.startswith("SELECT"):
            attempts.append(parameters)
            if len(attempts) == 1:
                raise QueryCanceled("transient query timeout")
    cursor.execute.side_effect = execute
    clock = iter([0., 0.1, 2.1, 2.2])
    rows = OptionStockBehaviorAssessmentRepository._technical_bars(cursor, ids, NOW, clock=lambda: next(clock))
    queries = [call for call in cursor.execute.call_args_list if call.args[0].startswith("SELECT")]
    assert len(queries) == 2 and all(call.args[1][0] == ids for call in queries)
    assert all(call.args[1][1:] == (NOW, NOW) for call in queries)
    assert all("NOT adjusted AND is_final" in call.args[0] and "quality_codes=ARRAY[]::text[]" in call.args[0] for call in queries)
    assert rows[0]["bar_revision_id"] == ids[0]
    timeouts = [call.args[1][0] for call in cursor.execute.call_args_list if len(call.args) > 1 and call.args[0].startswith("SET")]
    assert timeouts == [2000, 2900]
    assert any(call.args[0] == "ROLLBACK TO SAVEPOINT detector_technical_bar_read" for call in cursor.execute.call_args_list)
    cursor.reset_mock()
    attempts.clear()
    clock = iter([0., 0., 5.1])
    with pytest.raises(QueryCanceled, match="five-second budget"):
        OptionStockBehaviorAssessmentRepository._technical_bars(cursor, ids, NOW, clock=lambda: next(clock))
    assert sum(call.args[0].startswith("SELECT") for call in cursor.execute.call_args_list) == 1
    cursor.reset_mock()
    def fail(query, parameters=None):
        if query.startswith("SELECT"):
            raise ValueError("not a timeout")
    cursor.execute.side_effect = fail
    with pytest.raises(ValueError, match="not a timeout"):
        OptionStockBehaviorAssessmentRepository._technical_bars(cursor, ids, NOW, clock=lambda: 0)
    assert sum(call.args[0].startswith("SELECT") for call in cursor.execute.call_args_list) == 1


def test_detector_cycle_sources_read_only_exact_scope_and_bounds(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    inputs = detector_run_inputs()
    matrix = {**inputs["matrices"][0], "batch_id": uuid4()}
    cursor = MagicMock()
    @contextmanager
    def read():
        yield cursor
    repository = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    arguments = dict(configuration=inputs["configuration"], scheduled_cycle=inputs["scheduled_cycle"],
        completed_matrices={"AAPL": matrix["matrix_id"]}, as_of=inputs["selected_at"])
    cursor.fetchall.side_effect = [[matrix], [], [], []]
    result = repository.detector_cycle_sources(**arguments)
    assert result["matrices"] == (matrix,) and result["candidates"] == ()
    for call in cursor.execute.call_args_list:
        assert call.args[0].strip().startswith(("SET", "SELECT"))
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])
    cursor.fetchall.side_effect = [[], [], [], []]
    with pytest.raises(ValueError, match="exactly complete"):
        repository.detector_cycle_sources(**arguments)
    cursor.fetchall.side_effect = [[matrix], [dict(contract_id=1)] * 20001]
    with pytest.raises(ValueError, match="snapshot bound"):
        repository.detector_cycle_sources(**arguments)


def test_retained_detector_sources_bind_actual_receipts_and_dated_oi(monkeypatch):
    from dataclasses import asdict
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.detector_collection import RetainedDetectorSourceReader

    source, stock, inputs = surface_inputs()
    snapshot = inputs["snapshots"][0]
    clock = inputs["decision_at"]
    lineage = inputs["lineage"]
    matrix = {**asdict(lineage), "batch_id": snapshot.batch_id, "created_at": clock}
    fact = dict(contract_id=snapshot.contract_id, underlying=snapshot.underlyer,
        settlement_session=source.market_time.date() - timedelta(days=1), open_interest=snapshot.open_interest,
        open_interest_observed_at=snapshot.first_observed_at, open_interest_observed_session=source.market_time.date(),
        open_interest_batch_id=snapshot.batch_id, open_interest_source="PROVIDER_CHAIN_SNAPSHOT",
        created_at=clock, updated_at=clock, open_interest_revision_count=0)
    retained = dict(matrices=(matrix,), snapshots=({"created_at": clock},), open_interest=(fact,), candidates=())
    monkeypatch.setattr("options.repositories.snapshots._snapshot", lambda _: snapshot)
    repository = SimpleNamespace(detector_cycle_sources=MagicMock(return_value=retained))
    reader = RetainedDetectorSourceReader(repository,
        SimpleNamespace(get_security_as_of=lambda *_: inputs["security"]),
        SimpleNamespace(get_behavior_as_of=lambda *_, **__: stock), clock=lambda: clock)
    arguments = dict(configuration=detector_run_inputs()["configuration"], scheduled_cycle=lineage.scheduled_cycle,
        completed_matrices={"AAPL": lineage.matrix_id}, as_of=clock)
    from options.outcomes import configured_valuation_policy
    arguments["configuration"].valuation_policy = configured_valuation_policy()
    result = reader.read(**arguments)
    assert result["received_at"] == clock and len(result["activity"]) == 1
    assert result["activity"][0]["source"].oi_observed_at == fact["open_interest_observed_at"]
    fact["open_interest_revision_count"] = 1
    changed = reader.read(**arguments)
    assert changed["activity"][0]["source"].oi_observed_at is None
    assert "DATED_OPEN_INTEREST_UNAVAILABLE" in changed["activity"][0]["finding"].reasons
    fact["open_interest_revision_count"] = 0
    fact["updated_at"] = clock + timedelta(seconds=1)
    assert reader.read(**arguments)["activity"][0]["source"].oi_observed_at is None


def test_retained_surface_source_excludes_bad_point_without_poisoning_valid_group(monkeypatch):
    from dataclasses import asdict, replace
    from types import SimpleNamespace
    from options.detector_collection import RetainedDetectorSourceReader
    from options.domain import DataQualityFlag

    source, stock, inputs = surface_inputs()
    original = inputs["snapshots"][0]
    points = [replace(original, contract_id=point.contract_id, contract_ticker=point.contract_ticker,
        snapshot_id=point.snapshot_id, strike=point.strike, local_iv=point.local_iv,
        market_data_time=original.market_data_time + timedelta(seconds=index + 1),
        mark_market_data_time=original.market_data_time + timedelta(seconds=index + 1)) for index, point in enumerate(source.points)]
    points.append(replace(original, contract_id=9999, snapshot_id=uuid4(), quality_flags=(DataQualityFlag.FALLBACK_MARK,)))
    clock, lineage = inputs["decision_at"], inputs["lineage"]
    lineage = replace(lineage, market_time=max(point.market_data_time for point in points))
    retained = dict(matrices=({**asdict(lineage), "batch_id": original.batch_id, "created_at": clock},),
        snapshots=tuple(dict(snapshot=point, created_at=clock) for point in points), open_interest=(), candidates=())
    monkeypatch.setattr("options.repositories.snapshots._snapshot", lambda row: row["snapshot"])
    reader = RetainedDetectorSourceReader(SimpleNamespace(detector_cycle_sources=lambda **_: retained),
        SimpleNamespace(get_security_as_of=lambda *_: inputs["security"]),
        SimpleNamespace(get_behavior_as_of=lambda *_, **__: stock), clock=lambda: clock)
    from options.outcomes import configured_valuation_policy
    configuration = detector_run_inputs()["configuration"]
    configuration.valuation_policy = configured_valuation_policy()
    result = reader.read(configuration=configuration, scheduled_cycle=lineage.scheduled_cycle,
        completed_matrices={"AAPL": lineage.matrix_id}, as_of=clock)
    assert len(result["surface_inputs"]) == 1
    assert len(result["surface_inputs"][0]["source"].points) == len(source.points)
    assert result["surface_inputs"][0]["source"].schema_version == "option_local_surface_source_v2"
    assert result["rejections"]["SURFACE_POINT_INELIGIBLE"] == 1


def test_detector_collector_excludes_expired_packages_and_refuses_partial_sources():
    from types import SimpleNamespace
    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs

    source = qualification_inputs()
    lineage, decision = source["lineage"], source["decision"]
    matrices = {"AAPL": lineage.matrix_id}
    inputs = DetectorCycleInputs(matrices=(dict(underlying="AAPL", matrix_id=lineage.matrix_id,
        market_time=lineage.market_time, observed_time=lineage.observed_time),), package_inputs=(source,))
    reader = lambda **_: inputs
    collector = DetectorCycleCollector(reader, SimpleNamespace(prior_selected=lambda **_: {}),
        clock=lambda: source["entry_deadline"])
    arguments = dict(configuration=detector_run_inputs()["configuration"], dataset_id="expired-collector-fixture",
        scheduled_cycle=lineage.scheduled_cycle, completed_matrices=matrices, started_at=decision.decision_at)
    run, records = collector(**arguments)
    assert records == ()
    assert dict(run.rejections)["ORIGINAL_ENTRY_DEADLINE_OR_DECISION_CLOCK"] == 1
    with pytest.raises(ValueError, match="complete causal"):
        collector(**{**arguments, "completed_matrices": {}})
    collector.clock = lambda: decision.decision_at - timedelta(seconds=1)
    with pytest.raises(ValueError, match="clock precedes"):
        collector(**arguments)
    source["management_policy"] = None
    with pytest.raises(ValueError, match="management policy"):
        collector(**arguments)


def test_forward_detector_launch_rejects_unreviewed_policy_and_runtime(tmp_path):
    from options.config import load_option_runtime_configuration
    from options.detector_launch import DetectorForwardLaunch, RUNTIME_FILES, _source_hashes, validate_detector_forward_launch
    from test_equity_behavior_setup import publication_inputs, direct_policy
    from equity.behavior_setup import DirectResumptionSourcePolicy
    from pathlib import Path

    backend = Path(__file__).resolve().parents[2]
    configuration = load_option_runtime_configuration(dict(POLYGON_API_KEY="fixture",
        OPTION_STRATEGY_POLICY_FILE="options/policies/strategy_technical_forward_v1.json",
        OPTION_VALUATION_POLICY_FILE="options/policies/valuation_raw_spot_v2.json"), backend)
    _, _, _, shared = publication_inputs()
    acceptance = direct_policy(shared)
    resumption = DirectResumptionSourcePolicy.model_validate({**acceptance.model_dump(),
        "version": "stock_resumption_direct_source_v1", "detector_version": "relative_trend_resumption_intraday_v1"})
    launch = DetectorForwardLaunch(dataset_id="future-fixture-v1", effective_from=NOW,
        underlyers=configuration.settings.underlyers, configuration_sha256=configuration.configuration_sha256,
        strategy_policy_sha256=configuration.strategy_policy_sha256, valuation_policy_sha256=configuration.valuation_policy_sha256,
        stock_ledger="backups/missing.sqlite", acceptance_source=acceptance, resumption_source=resumption,
        runtime_sources=_source_hashes(backend, RUNTIME_FILES))
    with pytest.raises(ValueError, match="pinned stock runtime"):
        validate_detector_forward_launch(launch, configuration=configuration, backend_dir=backend)
    changed = launch.model_copy(update={"valuation_policy_sha256": "0" * 64})
    with pytest.raises(ValueError, match="reviewed configuration"):
        validate_detector_forward_launch(changed, configuration=configuration, backend_dir=backend)


def test_launch_preparation_requires_explicit_stock_transition_and_preserves_source(tmp_path, monkeypatch):
    import hashlib
    import json
    import sqlite3
    import zlib
    from pathlib import Path
    from options.config import load_option_runtime_configuration
    from options.detector_launch import INTRADAY_RUNTIME_FILES, OPTIMIZED_INTRADAY_RUNTIME_FILES, RUNTIME_FILES, prepare_detector_forward_launch, decode_detector_forward_launch, validate_detector_forward_launch
    from research.stock_idea_forward import forward_config, runtime_sources
    from research.stock_idea_engine import digest
    from test_equity_behavior_setup import direct_ledger_fixture

    path, _, publication, _ = direct_ledger_fixture(tmp_path)
    config = forward_config(quality_version=2)
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE forward_manifest SET policy_hash=?,payload=?", (digest(config), json.dumps(config)))
        publication["runtime_sources"] = {name: "0" * 64 for name in runtime_sources()}
        publication["policy_hash"] = digest(config)
        connection.execute("INSERT INTO forward_publications VALUES(?,?)", (publication["window_key"], zlib.compress(json.dumps(publication).encode())))
    for name in set(OPTIMIZED_INTRADAY_RUNTIME_FILES) | set(runtime_sources()):
        source = tmp_path / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("reviewed fixture", encoding="utf-8")
    configuration = load_option_runtime_configuration(dict(POLYGON_API_KEY="fixture",
        OPTION_STRATEGY_POLICY_FILE="options/policies/strategy_technical_forward_v1.json",
        OPTION_VALUATION_POLICY_FILE="options/policies/valuation_raw_spot_v2.json"), Path(__file__).resolve().parents[2])
    arguments = dict(backend_dir=tmp_path, configuration=configuration, dataset_id="parallel-start-fixture",
        effective_from=NOW, stock_ledger=path.name)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="revalidation required"):
        prepare_detector_forward_launch(**arguments)
    launch = prepare_detector_forward_launch(**arguments, approve_stock_runtime_transition=True)
    assert launch.schema_version == "option_detector_forward_launch_v2"
    assert decode_detector_forward_launch(launch.canonical_json()) == launch
    assert dict(launch.acceptance_source.runtime_sources) != publication["runtime_sources"]
    assert launch.acceptance_source.runtime_sources == launch.resumption_source.runtime_sources
    wait_launch = prepare_detector_forward_launch(**arguments, approve_stock_runtime_transition=True,
        intraday_confirmation=True, stock_setup_wait=True)
    assert wait_launch.schema_version == "option_detector_forward_launch_v6"
    assert wait_launch.stock_readiness_policy == "PER_MODEL_EXACT_WINDOW_WAIT_V2"
    assert decode_detector_forward_launch(wait_launch.canonical_json()) == wait_launch
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    original_payload = json.loads(launch.canonical_json())
    original_payload.pop("stock_readiness_policy")
    original_payload["schema_version"] = "option_detector_forward_launch_v1"
    original = decode_detector_forward_launch(json.dumps(original_payload))
    assert decode_detector_forward_launch(original.canonical_json()).sha256 == original.sha256
    missing_source = launch.model_copy(update={"stock_ledger": "absent.sqlite"})
    assert validate_detector_forward_launch(missing_source, configuration=configuration, backend_dir=tmp_path) == missing_source
    assert not (tmp_path / "absent.sqlite").exists()
    with pytest.raises(ValueError, match="ledger is unavailable"):
        validate_detector_forward_launch(original.model_copy(update={"stock_ledger": "absent.sqlite"}), configuration=configuration, backend_dir=tmp_path)
    monkeypatch.setattr("research.stock_idea_forward.forward_config", lambda **_: dict(config, policy_version="changed"))
    with pytest.raises(ValueError, match="enrolled policy"):
        prepare_detector_forward_launch(**arguments, approve_stock_runtime_transition=True)


@pytest.mark.parametrize("cutoff,expected", [
    ("2026-09-21T13:59:59+00:00", None),
    ("2026-09-21T14:00:00+00:00", "2026-09-21T14:00:00+00:00"),
    ("2026-09-21T14:29:59+00:00", "2026-09-21T14:00:00+00:00"),
    ("2026-09-21T14:30:00+00:00", "2026-09-21T14:30:00+00:00"),
    ("2026-11-27T18:20:00+00:00", "2026-11-27T18:00:00+00:00"),
    ("2026-09-20T15:00:00+00:00", None),
])
def test_stock_windows_follow_market_clock_without_double_provider_delay(cutoff, expected):
    from options.detector_launch import aligned_stock_windows

    cutoff = datetime.fromisoformat(cutoff)
    result = aligned_stock_windows({"AAPL": cutoff})["AAPL"]
    assert result == (cutoff, datetime.fromisoformat(expected) if expected else None)


def test_exact_stock_publication_wait_is_bounded_and_heartbeat_safe():
    from collections import Counter
    from options.detector_launch import read_stock_setup_windows_when_ready

    boundary = datetime(2026, 9, 23, 14, 30, tzinfo=timezone.utc)
    current = [boundary + timedelta(minutes=23)]
    reads, heartbeats = [], []
    def reader(_path, **arguments):
        reads.append(arguments["as_of"])
        if len(reads) < 3:
            return (), Counter({"S1_STOCK_WINDOW_NOT_AVAILABLE": 1, "S2_STOCK_WINDOW_NOT_AVAILABLE": 1})
        return ("setup",), Counter()
    def wait(seconds):
        current[0] += timedelta(seconds=seconds)

    evidence, reasons = read_stock_setup_windows_when_ready(reader, path="unused",
        policies=("policy",), windows={"AAPL": (current[0], boundary)}, as_of=current[0],
        wait_deadline=boundary + timedelta(minutes=29), progress_callback=lambda: heartbeats.append(current[0]),
        clock=lambda: current[0], wait=wait)
    assert evidence == ("setup",) and reasons == Counter()
    assert len(reads) == 3 and len(heartbeats) == 2
    assert reads == sorted(reads) and reads[-1] <= boundary + timedelta(minutes=29)

    current[0] = boundary + timedelta(minutes=29, seconds=54)
    reads.clear()
    evidence, reasons = read_stock_setup_windows_when_ready(lambda *args, **kwargs: (
        (), Counter({"S1_STOCK_WINDOW_NOT_AVAILABLE": 1})), path="unused", policies=("policy",),
        windows={"AAPL": (current[0], boundary)}, as_of=current[0], wait_deadline=boundary + timedelta(hours=1),
        clock=lambda: current[0], wait=wait)
    assert evidence == () and reasons["S1_STOCK_WINDOW_NOT_AVAILABLE"] == 1
    assert current[0] == boundary + timedelta(minutes=29, seconds=55)

    reads.clear()
    heartbeats.clear()
    read_stock_setup_windows_when_ready(reader, path="unused", policies=("policy",),
        windows={"AAPL": (current[0], None)}, as_of=current[0],
        wait_deadline=current[0] + timedelta(minutes=10), progress_callback=lambda: heartbeats.append(current[0]),
        clock=lambda: current[0], wait=wait)
    assert len(reads) == 1 and not heartbeats


def test_forward_worker_refuses_older_slots_without_provider_work():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from options.worker import OptionMaterializationWorker, OptionWorkerSettings

    pipeline = MagicMock()
    worker = OptionMaterializationWorker(pipeline, calendar=SimpleNamespace(latest_delayed_slot=lambda *_, **__: MARKET),
        settings=OptionWorkerSettings(), effective_from=MARKET + timedelta(days=3), clock=lambda: NOW)
    assert worker.poll_once() is None
    pipeline.run_once.assert_not_called()


def test_forward_launch_worker_default_is_disabled_and_requires_checksum(monkeypatch):
    from scripts.run_option_worker import _detector_pipeline_arguments

    monkeypatch.delenv("OPTION_TECHNICAL_FORWARD_LAUNCH_FILE", raising=False)
    monkeypatch.delenv("OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256", raising=False)
    assert _detector_pipeline_arguments(None) == {}
    monkeypatch.setenv("OPTION_TECHNICAL_FORWARD_LAUNCH_FILE", "research/fixture.json")
    with pytest.raises(ValueError, match="both manifest"):
        _detector_pipeline_arguments(None)


def test_forward_launch_storage_requires_shadow_observation_guards():
    from scripts.run_option_worker import _detector_storage_ready

    ready = dict(registered=True, runtime_select=True, runtime_insert=True,
        o1_table_present=True, o1_registered=True, o1_runtime_select=True, o1_runtime_insert=True,
        stock_setup_table_present=True, stock_setup_registered=True,
        stock_setup_runtime_select=True, stock_setup_runtime_insert=True,
        unvalidated_constraints=0,
        triggers=[{"enabled": "O"}] * 3, o1_triggers=[{"enabled": "O"}] * 3,
        stock_setup_triggers=[{"enabled": "O"}] * 3,
        indexes=[{"valid": True, "ready": True}] * 4)
    assert _detector_storage_ready(ready)
    for field in ("o1_table_present", "o1_registered", "o1_runtime_select", "o1_runtime_insert"):
        assert not _detector_storage_ready({**ready, field: False})
    for field in ("stock_setup_table_present", "stock_setup_registered",
                  "stock_setup_runtime_select", "stock_setup_runtime_insert"):
        assert not _detector_storage_ready({**ready, field: False})
    assert not _detector_storage_ready({**ready, "o1_triggers": ready["o1_triggers"][:2]})
    assert not _detector_storage_ready({**ready, "o1_triggers": [{"enabled": "D"}] * 3})
    assert not _detector_storage_ready({**ready, "stock_setup_triggers": ready["stock_setup_triggers"][:2]})
    assert not _detector_storage_ready({**ready, "stock_setup_triggers": [{"enabled": "D"}] * 3})


def test_detector_source_report_forbids_output_and_mixed_modes(monkeypatch):
    from scripts.report_stock_behavior_coverage import parse_args

    for arguments in (("--output", "unused.json"), ("--detector-schema",), ("--setup-publications",)):
        monkeypatch.setattr("sys.argv", ["report", "--detector-sources", *arguments])
        with pytest.raises(SystemExit):
            parse_args()
    monkeypatch.setattr("sys.argv", ["report", "--detector-sources", "--compact", "--output", "validation.json"])
    compact = parse_args()
    assert compact.detector_sources and compact.compact and compact.output.name == "validation.json"
    monkeypatch.setattr("sys.argv", ["report", "--detector-sources"])
    assert parse_args().detector_sources


def test_o1_indicator_period_report_requires_an_isolated_bounded_mode(monkeypatch):
    from scripts.report_stock_behavior_coverage import parse_args

    monkeypatch.setattr("sys.argv", ["report", "--o1-indicator-review", "2026-09-22", "2026-09-29",
        "--detector-dataset-id", "options-shadow-v10"])
    arguments = parse_args()
    assert arguments.o1_indicator_review == [date(2026, 9, 22), date(2026, 9, 29)]
    assert arguments.detector_dataset_id == "options-shadow-v10"
    monkeypatch.setattr("sys.argv", ["report", "--detector-dataset-id", "options-shadow-v10"])
    with pytest.raises(SystemExit):
        parse_args()


def test_stock_setup_indicator_period_report_accepts_exact_dataset_scope(monkeypatch):
    from scripts.report_stock_behavior_coverage import parse_args

    monkeypatch.setattr("sys.argv", ["report", "--stock-setup-indicator-review", "2026-09-22", "2026-09-29",
        "--detector-dataset-id", "options-mixed-v11"])
    arguments = parse_args()
    assert arguments.stock_setup_indicator_review == [date(2026, 9, 22), date(2026, 9, 29)]
    assert arguments.detector_dataset_id == "options-mixed-v11"
    monkeypatch.setattr("sys.argv", ["report", "--o1-indicator-review", "2026-09-22", "2026-09-29", "--detector-schema"])
    with pytest.raises(SystemExit):
        parse_args()


def test_complete_run_repository_refuses_partial_source_and_changed_retry(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.analytics.alert_selection import build_detector_run
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    run = build_detector_run(**detector_run_inputs())
    cursor = MagicMock()
    @contextmanager
    def write():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", write)
    cursor.fetchone.side_effect = [None, None]
    cursor.fetchall.return_value = []
    with pytest.raises(ValueError, match="exactly complete"):
        repository.persist_completed_run(run, ())
    assert not any("INSERT" in call.args[0] for call in cursor.execute.call_args_list)
    cursor.fetchone.side_effect = [dict(publication_id=run.run_id, selector_sha256=run.scope_sha256,
        scheduled_cycle=run.scheduled_cycle, published_at=run.selected_at,
        configuration_sha256=run.configuration_sha256, strategy_policy_sha256=run.strategy_policy_sha256,
        source_matrix_ids=[matrix for _, matrix in run.source_matrices], covered_underlying_count=len(run.expected_underlyers),
        selection_evidence=dict(kind="DUAL_ORIGIN_COMPLETE_RUN", dataset_id=run.dataset_id,
            payload_text=run.canonical_json(), payload_sha256=run.sha256))]
    assert repository.persist_completed_run(run, ())["status"] == "ALREADY_RECORDED"


def test_completed_run_reader_reconciles_exact_records(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.analytics.alert_selection import build_detector_run
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    records = evaluation_records()
    run = build_detector_run(**detector_run_inputs(records))
    header = dict(publication_id=run.run_id, selector_sha256=run.scope_sha256, scheduled_cycle=run.scheduled_cycle,
        published_at=run.selected_at, configuration_sha256=run.configuration_sha256,
        strategy_policy_sha256=run.strategy_policy_sha256, source_matrix_ids=[matrix for _, matrix in run.source_matrices],
        covered_underlying_count=len(run.expected_underlyers), selection_evidence=dict(kind="DUAL_ORIGIN_COMPLETE_RUN",
            dataset_id=run.dataset_id, payload_text=run.canonical_json(), payload_sha256=run.sha256))
    cursor = MagicMock()
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    arguments = dict(dataset_id=run.dataset_id, scheduled_cycle=run.scheduled_cycle, as_of=run.selected_at)
    cursor.fetchall.side_effect = [[header], [dict(evaluation_id=row.evaluation_id,
        payload_text=row.canonical_json(), payload_sha256=row.sha256) for row in records]]
    assert repository.completed_run(**arguments) == (run, records)
    cursor.fetchall.side_effect = [[header], []]
    with pytest.raises(ValueError, match="missing or has changed"):
        repository.completed_run(**arguments)
    cursor.fetchall.side_effect = [[]]
    assert repository.completed_run(**arguments) is None


def test_evaluation_repository_idempotence_and_no_silent_run_changes(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    records = evaluation_records()
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    @contextmanager
    def transaction():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", transaction)
    inserts = MagicMock()
    monkeypatch.setattr("options.repositories.alert_evaluations.execute_values", inserts)
    assert repository.persist_run(records) == 1
    inserts.assert_called_once()
    cursor.fetchall.return_value = [dict(evaluation_id=records[0].evaluation_id,
        payload_text=records[0].canonical_json(), payload_sha256=records[0].sha256)]
    assert repository.persist_run(records) == 0
    cursor.fetchall.return_value[0]["payload_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="differs"):
        repository.persist_run(records)


def test_evaluation_reader_missing_schema_is_read_only(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": False}
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    result = repository.review_inputs(as_of=NOW)
    assert not result["ready"] and result["records"] == []
    assert "READ ONLY" in cursor.execute.call_args_list[0].args[0]
    assert all(call.args[0].strip().startswith(("SET", "SELECT")) for call in cursor.execute.call_args_list)


def test_eod_selection_comparison_preserves_exclusions_and_missing_outcomes():
    from types import SimpleNamespace
    from options.analytics.alert_selection import DetectorSelectionEvidence
    from options.analytics.behavior_review import build_detector_evaluation_review

    selected = evaluation_records()[0]
    overflow = DetectorSelectionEvidence.model_validate({**evaluation_records()[0].model_dump(),
        "selection_status": "NOT_SELECTED", "selection_reason": "RUN_CAP"})
    repository = SimpleNamespace(review_inputs=lambda **_: dict(ready=True, dataset_id=selected.dataset_id,
        datasets=[selected.dataset_id], sessions=[NOW.date()], session_date=NOW.date(), records=[selected, overflow]))
    result = build_detector_evaluation_review(as_of=NOW, repository=repository, selection_status="NOT_SELECTED")
    assert result["total"] == 1 and result["rows"][0]["selection_reason"] == "RUN_CAP"
    assert {cell["selection_status"] for cell in result["cells"]} == {"SELECTED", "NOT_SELECTED"}
    assert all(cell["measured"] is None and cell["positive_rate"] is None for cell in result["cells"])
    assert result["rows"][0]["net_return"] is None
    assert result["maximum_new_alerts"] == 20
    assert [model["detector_id"] for model in result["models"]] == ["O1", "O2", "S1", "S2"]
    assert result["models"][0]["selected"] == 1 and result["models"][0]["not_selected"] == 1
    assert all(model["measured"] is None for model in result["models"])


def test_detector_alert_reader_counts_detected_o2_without_exposing_alert_rows():
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_surface_evidence
    from options.analytics.behavior_review import build_detector_alert_review
    from options.surface_detection import assess_surface_first

    source, stock, inputs = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    record = build_surface_evidence((observation,), dataset_id="observation-ui", selected_at=inputs["decision_at"])[0]
    run = SimpleNamespace(run_id=record.run_id, scheduled_cycle=record.scheduled_cycle,
        selected_at=record.selected_at, market_time=source.market_time, observed_time=source.observed_at,
        expected_underlyers=(source.underlyer,), source_matrices=((source.underlyer, source.matrix_id),),
        selection_counts=(("SELECTED", 0), ("REPEAT", 0), ("NOT_SELECTED", 0), ("OBSERVATION", 1)), rejections=())
    calls = []
    repo = SimpleNamespace(completed_runs=lambda **_: (run,), completed_run=lambda **_: (run, (record,)),
        repeat_counts=lambda **kwargs: calls.append(kwargs["records"]) or {})
    review = build_detector_alert_review(dataset_id=record.dataset_id, as_of=record.selected_at,
        detector="O2", repository=repo)
    assert review["total"] == 0 and review["detected_observations"] == 1
    assert review["rows"] == []
    assert calls == [[]]
    assert build_detector_alert_review(dataset_id=record.dataset_id, as_of=record.selected_at,
        detector="O1", repository=repo)["total"] == 0
    empty = record.model_copy(update={"observation": observation.model_copy(update={"findings": (), "finding_disposition": "NOT_DETECTED"})})
    repo.completed_run = lambda **_: (run, (empty,))
    assert build_detector_alert_review(dataset_id=record.dataset_id, as_of=record.selected_at,
        repository=repo)["total"] == 0


def test_detector_alert_reader_rejects_latest_run_above_alert_cap():
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_detector_run
    from options.analytics.behavior_review import build_detector_alert_review

    record = evaluation_records()[0]
    records = (record,) * 21
    run = build_detector_run(**detector_run_inputs((record,)))
    repository = SimpleNamespace(completed_runs=lambda **_: (run,),
        completed_run=lambda **_: (run, records))

    with pytest.raises(ValueError, match="exceeds maximum new alerts"):
        build_detector_alert_review(dataset_id=record.dataset_id,
            as_of=record.selected_at, repository=repository)


def test_detector_alert_reader_zero_latest_keeps_original_history_and_hits(monkeypatch):
    import json
    from types import SimpleNamespace
    from options.analytics.alert_selection import DetectorRunEvidence, build_detector_run
    from options.analytics.behavior_review import build_detector_alert_review

    records = evaluation_records()
    first = build_detector_run(**detector_run_inputs(records))
    latest = DetectorRunEvidence.model_validate({**first.model_dump(),
        "scheduled_cycle": first.scheduled_cycle + timedelta(minutes=15),
        "selected_at": first.selected_at + timedelta(minutes=15), "record_sha256s": (),
        "rejections": (("O1_STOCK_CONFIRMATION_UNAVAILABLE", 5), ("SURFACE_POINT_INELIGIBLE", 20)),
        "selection_counts": tuple((status, 0) for status, _ in first.selection_counts)})
    repo = SimpleNamespace(completed_runs=lambda **_: (first, latest),
        completed_run=lambda **_: (latest, ()),
        review_inputs=lambda **_: dict(runs=(first, latest), records=records),
        repeat_counts=lambda **_: {records[0].evaluation_id: dict(repeats=3, last_seen_at=latest.selected_at)})
    original = dict(status="AVAILABLE", market_data_time=records[0].package.decision_at,
        net_premium=Decimal("-100"), capital_at_risk=Decimal("100"), legs=[])
    source_repository = SimpleNamespace(original_packages=lambda page, **_: {
        str(records[0].candidate_id): original} if page else {})
    mark_calls = []
    mark_repository = SimpleNamespace(retained_plan_marks=lambda ids, **kwargs: (
        mark_calls.append((ids, kwargs)) or {"ready": True, "rows": {
            str(records[0].candidate_id): {"candidate_id": records[0].candidate_id}}}))
    expected_mark = dict(status="FRESH", reason=None, package_price=Decimal("2.5"),
        price_return=Decimal("0.25"), market_time=latest.selected_at)
    monkeypatch.setattr("options.outcomes.review_retained_candidate_mark",
        lambda candidate, retained, **_: expected_mark)
    valuation = SimpleNamespace(policy_sha256="e" * 64)
    current = build_detector_alert_review(dataset_id=first.dataset_id, as_of=latest.selected_at,
        repository=repo, source_repository=source_repository, valuation_policy=valuation,
        mark_repository=mark_repository)
    assert current["status"] == "COMPLETE" and current["rows"] == [] and current["new_alerts"] == 0
    assert mark_calls == []
    assert current["run"]["published_at"] == latest.selected_at.isoformat()
    assert current["run"]["scheduled_cycle"] == latest.scheduled_cycle.isoformat()
    assert current["run"]["published_at"] != current["run"]["scheduled_cycle"]
    assert current["run"]["rejections"] == dict(latest.rejections)
    assert current["run"]["covered_underlyings"] == current["run"]["expected_underlyings"]
    history = build_detector_alert_review(dataset_id=first.dataset_id, as_of=latest.selected_at,
        repository=repo, scope="HISTORY", source_repository=source_repository,
        valuation_policy=valuation, mark_repository=mark_repository)
    assert history["run"] is None
    assert [run["run_id"] for run in history["runs"]] == [str(first.run_id)]
    assert history["latest_run"]["published_at"] == latest.selected_at.isoformat()
    assert history["rows"][0]["hit_count"] == 4
    assert history["rows"][0]["plan_sha256"] == records[0].package.plan_sha256
    assert history["rows"][0]["first_selected_at"] == first.selected_at.isoformat()
    assert history["rows"][0]["current_mark"] == expected_mark
    assert mark_calls == [([records[0].candidate_id], dict(available_by=latest.selected_at,
        valuation_policy_sha256="e" * 64))]
    assert datetime.fromisoformat(history["rows"][0]["triggered_at"]) == datetime.fromisoformat(json.loads(records[0].plan_payload_text)["source_market_time"])
    assert history["rows"][0]["net_return"] is None and history["rows"][0]["fill"] is None
    repo.completed_runs = lambda **_: ()
    empty = build_detector_alert_review(dataset_id="different-dataset", as_of=latest.selected_at, repository=repo)
    assert empty["status"] == "NO_COMPLETE_RUN" and empty["rows"] == []
    assert empty["run"] is None and empty["latest_run"] is None and empty["runs"] == []


@pytest.mark.parametrize("model", ["O1", "S1", "S2"])
def test_detector_trigger_time_uses_original_origin_evidence_and_missing_sorts_last(model):
    import json
    from types import SimpleNamespace
    from options.analytics.behavior_review import _detector_triggered_at, _sort_detector_records

    option_time, stock_time = NOW - timedelta(minutes=5), NOW - timedelta(minutes=20)
    plan = dict(source_market_time=option_time.isoformat(), management_policy=dict(technical_exit=dict(
        evidence=dict(source_kind="STOCK_SETUP", market_time=stock_time.isoformat()))))
    record = SimpleNamespace(detector_id=model, plan_payload_text=json.dumps(plan), package=SimpleNamespace(decision_at=NOW),
        evaluation_id=uuid4(), scheduled_cycle=NOW - timedelta(minutes=15), selected_at=NOW)
    assert _detector_triggered_at(record) == (option_time if model == "O1" else stock_time)
    missing = SimpleNamespace(**{**vars(record), "evaluation_id": uuid4(), "plan_payload_text": "{}"})
    for order in ("asc", "desc"):
        assert _sort_detector_records([missing, record], "triggered_at", order) == [record, missing]
    for value in (None, "invalid", NOW.replace(tzinfo=None).isoformat(), (NOW + timedelta(seconds=1)).isoformat()):
        plan["source_market_time"] = value
        plan["management_policy"]["technical_exit"]["evidence"]["market_time"] = value
        record.plan_payload_text = json.dumps(plan)
        assert _detector_triggered_at(record) is None


def test_detector_readers_sort_package_strategy_before_pagination():
    from types import SimpleNamespace
    from options.dual_origin import qualify_dual_origin_package
    from options.analytics.alert_selection import build_selection_evidence, select_dual_origin_packages, build_detector_run
    from options.analytics.behavior_review import build_detector_evaluation_review, build_detector_alert_review

    records = []
    for structure in ("LONG_CALL", "CALL_DEBIT_VERTICAL"):
        package, plan = qualify_dual_origin_package(**qualification_inputs(structure))
        selection = select_dual_origin_packages([package], {}, decision_at=package.decision_at,
            scheduled_cycle=package.scheduled_cycle, expected_underlyers=(package.underlyer,), completed_matrices={package.underlyer: package.matrix_id})
        records.extend(build_selection_evidence(selection, {plan.sha256: plan}, dataset_id="sort-fixture", selected_at=package.decision_at))
    run = build_detector_run(**detector_run_inputs((records[0],)))
    repo = SimpleNamespace(review_inputs=lambda **_: dict(ready=True, datasets=[run.dataset_id], dataset_id=run.dataset_id,
        sessions=[run.scheduled_cycle.date()], session_date=run.scheduled_cycle.date(), records=records),
        completed_runs=lambda **_: (run,), completed_run=lambda **_: (run, tuple(records)),
        repeat_counts=lambda **kwargs: {row.evaluation_id: dict(repeats=0, last_seen_at=None) for row in kwargs["records"]})
    for build in (build_detector_evaluation_review, build_detector_alert_review):
        args = dict(dataset_id=run.dataset_id, as_of=run.selected_at, repository=repo, sort_by="strategy", limit=1)
        assert build(**args)["rows"][0]["strategy_name"] == "DIRECTIONAL_DEBIT_SPREAD"
        assert build(**args, offset=1)["rows"][0]["strategy_name"] == "DIRECTIONAL_LONG_PREMIUM"
        assert build(**args, sort_order="desc")["rows"][0]["strategy_name"] == "DIRECTIONAL_LONG_PREMIUM"
        assert build(**args, underlyer="MSFT")["total"] == 0
        assert build(**args, underlyer="aapl")["total"] == 2
        with pytest.raises(ValueError, match="sort"):
            build(**{**args, "sort_by": "unknown"})


def test_detector_alert_latest_uses_selected_session_without_cross_day_fallback():
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_detector_run, DetectorRunEvidence
    from options.analytics.behavior_review import build_detector_alert_review

    first = build_detector_run(**detector_run_inputs())
    second = DetectorRunEvidence.model_validate({**first.model_dump(),
        "scheduled_cycle": first.scheduled_cycle + timedelta(days=3), "selected_at": first.selected_at + timedelta(days=3)})
    repo = SimpleNamespace(completed_runs=lambda **_: (first, second),
        completed_run=lambda **kwargs: (first if kwargs["scheduled_cycle"] == first.scheduled_cycle else second, ()),
        repeat_counts=lambda **_: {})
    arguments = dict(dataset_id=first.dataset_id, as_of=second.selected_at, repository=repo)
    result = build_detector_alert_review(**arguments, session_date=first.scheduled_cycle.date())
    assert result["latest_run_id"] == str(first.run_id)
    missing = build_detector_alert_review(**arguments, session_date=first.scheduled_cycle.date() + timedelta(days=1))
    assert missing["status"] == "NO_COMPLETE_RUN" and missing["rows"] == []
    assert len(missing["sessions"]) == 2


def test_detector_sort_keeps_observation_package_fields_last():
    from options.analytics.alert_selection import build_surface_evidence
    from options.analytics.behavior_review import _sort_detector_records
    from options.surface_detection import assess_surface_first

    source, stock, inputs = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    record = build_surface_evidence([observation], dataset_id="sort-fixture", selected_at=inputs["decision_at"])[0]
    selected = evaluation_records()[0]
    for field in ("strategy", "entry_limit", "rank"):
        for order in ("asc", "desc"):
            assert _sort_detector_records([record, selected], field, order) == [selected, record]


def test_alert_original_package_reader_binds_retained_snapshot_and_preserves_missing(monkeypatch):
    from contextlib import contextmanager
    from dataclasses import asdict
    from unittest.mock import MagicMock
    from options.dual_origin import qualify_dual_origin_package
    from options.analytics.alert_selection import build_selection_evidence, select_dual_origin_packages
    from options.repositories.alert_review_sources import OptionAlertReviewSourceRepository

    inputs = qualification_inputs()
    package, plan = qualify_dual_origin_package(**inputs)
    selection = select_dual_origin_packages([package], {}, decision_at=package.decision_at, scheduled_cycle=package.scheduled_cycle,
        expected_underlyers=(package.underlyer,), completed_matrices={package.underlyer: package.matrix_id})
    records = build_selection_evidence(selection, {plan.sha256: plan}, dataset_id="display-fixture", selected_at=package.decision_at)
    candidate = asdict(inputs["candidate"])
    candidate.update(candidate_identity=candidate.pop("identity_sha256"), underlying=candidate.pop("underlyer"),
        candidate_rank=candidate.pop("rank"), created_at=package.decision_at)
    legs = []
    for leg, snapshot in zip(candidate.pop("legs"), inputs["snapshots"]):
        legs.append(dict(leg, candidate_id=package.candidate_id, matched_snapshot_id=snapshot.snapshot_id,
            snapshot_contract_id=snapshot.contract_id, snapshot_underlying=snapshot.underlyer,
            batch_id=snapshot.batch_id, normalized_payload_sha256="a" * 64,
            snapshot_mark=snapshot.model_mark, snapshot_spot=snapshot.spot, snapshot_iv=snapshot.local_iv,
            snapshot_valuation_sha256=snapshot.valuation_policy_sha256, first_observed_at=snapshot.first_observed_at,
            revised_observed_at=None, day_volume=0, open_interest=None, quote_bid=None, quote_ask=None))
    cursor = MagicMock()
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertReviewSourceRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    cursor.fetchall.side_effect = [[candidate], legs]
    result = repository.original_packages(records, as_of=package.decision_at)[str(package.candidate_id)]
    assert result["legs"][0]["day_volume"] == 0 and result["legs"][0]["open_interest"] is None
    assert result["legs"][0]["valuation_policy_sha256"] == inputs["candidate"].legs[0].valuation_policy_sha256
    assert result["legs"][0]["model_version"] == inputs["candidate"].legs[0].model_version
    assert result["legs"][0]["quality_flags"] == inputs["candidate"].legs[0].quality_flags
    assert result["net_premium"] == inputs["candidate"].net_premium
    for call in cursor.execute.call_args_list:
        assert call.args[0].strip().startswith(("SET", "SELECT"))
        if len(call.args) > 1: assert call.args[0].count("%s") == len(call.args[1])
    legs[0]["snapshot_mark"] += 1
    cursor.fetchall.side_effect = [[candidate], legs]
    with pytest.raises(ValueError, match="snapshot binding"):
        repository.original_packages(records, as_of=package.decision_at)
    cursor.fetchall.side_effect = [[], []]
    assert repository.original_packages(records, as_of=package.decision_at) == {}


@pytest.mark.parametrize("now,effective,completed,start,missing", [
    ("2026-09-21T13:00:00+00:00", "2026-09-21T13:30:00+00:00", [], "2026-09-21T14:00:30+00:00", 0),
    ("2026-09-21T14:05:00+00:00", "2026-09-21T13:30:00+00:00", [], "2026-09-21T14:00:30+00:00", 0),
    ("2026-09-21T14:16:00+00:00", "2026-09-21T13:30:00+00:00", [], "2026-09-21T14:15:30+00:00", 1),
    ("2026-09-21T14:16:00+00:00", "2026-09-21T13:30:00+00:00", ["2026-09-21T13:45:00+00:00", "2026-09-21T14:00:00+00:00"], "2026-09-21T14:30:30+00:00", 0),
    ("2026-09-21T14:50:00+00:00", "2026-09-21T15:00:00+00:00", [], "2026-09-21T15:15:30+00:00", 0),
    ("2026-09-20T15:00:00+00:00", "2026-09-18T13:30:00+00:00", [], "2026-09-21T14:00:30+00:00", 0),
    ("2026-11-27T18:31:00+00:00", "2026-11-27T18:00:00+00:00", [], "2026-11-30T15:00:30+00:00", 1),
])
def test_detector_schedule_distinguishes_windows_from_completed_runs(now, effective, completed, start, missing):
    from options.repositories.alert_review_sources import detector_alert_schedule
    from options.worker import OptionWorkerSettings

    result = detector_alert_schedule(as_of=datetime.fromisoformat(now), effective_from=datetime.fromisoformat(effective),
        settings=OptionWorkerSettings(), completed=[datetime.fromisoformat(value) for value in completed])
    assert datetime.fromisoformat(result["window_start"]) == datetime.fromisoformat(start)
    assert result["unpublished_windows"] == missing and result["warning"] == bool(missing)
    assert result["dataset_timing"] == "EXPECTED_CHECK_WINDOW_NOT_COMPLETION_DEADLINE"


def test_current_detector_dataset_is_default_even_before_any_run(monkeypatch):
    from options import api
    from options.repositories import alert_review_sources
    from options.analytics import behavior_review

    monkeypatch.setattr(alert_review_sources, "configured_detector_dataset", lambda: "current-four-model-v1")
    monkeypatch.setattr(alert_review_sources.OptionAlertReviewSourceRepository, "dataset_index", lambda *_, **__: dict(storage_ready=True, datasets=["old-v1"]))
    response = api.option_detector_datasets()
    assert response.data["default_dataset_id"] == "current-four-model-v1"
    assert response.data["datasets"] == ["current-four-model-v1", "old-v1"]
    calls = []
    monkeypatch.setattr(behavior_review, "build_detector_evaluation_review", lambda **kwargs: calls.append(kwargs) or {})
    api.option_alert_evaluations(dataset_id=None, limit=50, offset=0)
    assert calls[-1]["dataset_id"] == "current-four-model-v1"
    api.option_alert_evaluations(dataset_id="old-v1", limit=50, offset=0)
    assert calls[-1]["dataset_id"] == "old-v1"


def test_detector_dataset_manifest_is_read_only_and_hash_checked():
    from pathlib import Path
    from options.repositories.alert_review_sources import configured_detector_dataset

    assert configured_detector_dataset(environ={}) is None
    with pytest.raises(ValueError, match="incomplete"):
        configured_detector_dataset(environ={"OPTION_TECHNICAL_FORWARD_LAUNCH_FILE": "missing.json"})
    root = Path(__file__).resolve().parents[2]
    with pytest.raises(ValueError, match="path or size"):
        configured_detector_dataset(backend_dir=root, environ={"OPTION_TECHNICAL_FORWARD_LAUNCH_FILE": "../README.md", "OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256": "a" * 64})


def test_detector_dataset_default_prefers_reviewed_dotenv_pins(tmp_path, monkeypatch):
    from options.detector_launch import _source_hashes, StockSetupReadyDetectorForwardLaunch, INTRADAY_RUNTIME_FILES
    from options.repositories.alert_review_sources import configured_detector_dataset
    from options.intraday_participation import INTRADAY_ALIGNMENT_POLICY
    from test_equity_behavior_setup import direct_policy, publication_inputs
    from equity.behavior_setup import DirectResumptionSourcePolicy

    for name in INTRADAY_RUNTIME_FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture", encoding="utf-8")
    _, _, _, original_policy = publication_inputs()
    acceptance = direct_policy(original_policy)
    resumption = DirectResumptionSourcePolicy(instance_id=acceptance.instance_id,
        instance_policy_sha256=acceptance.instance_policy_sha256,
        publication_policy_sha256=acceptance.publication_policy_sha256,
        runtime_sources=acceptance.runtime_sources)
    launch = StockSetupReadyDetectorForwardLaunch(dataset_id="dotenv-current", effective_from=NOW,
        underlyers=("AAPL",), configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64,
        valuation_policy_sha256="c" * 64, stock_ledger="backups/fixture.sqlite",
        acceptance_source=acceptance, resumption_source=resumption,
        runtime_sources=_source_hashes(tmp_path, INTRADAY_RUNTIME_FILES),
        o1_confirmation_policy_sha256=INTRADAY_ALIGNMENT_POLICY.sha256)
    manifest = tmp_path / "launch.json"
    manifest.write_text(launch.canonical_json(), encoding="utf-8")
    (tmp_path / ".env").write_text(
        f"OPTION_TECHNICAL_FORWARD_LAUNCH_FILE=launch.json\nOPTION_TECHNICAL_FORWARD_LAUNCH_SHA256={launch.sha256}\n",
        encoding="utf-8")
    monkeypatch.setenv("OPTION_TECHNICAL_FORWARD_LAUNCH_FILE", "stale.json")
    monkeypatch.setenv("OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256", "0" * 64)

    assert configured_detector_dataset(backend_dir=tmp_path) == "dotenv-current"


def test_detector_alert_api_requires_dataset_and_only_dispatches_reader(monkeypatch):
    from options import api
    from options.analytics import behavior_review

    calls = []
    monkeypatch.setattr(behavior_review, "build_detector_alert_review", lambda **kwargs: calls.append(kwargs) or dict(rows=[]))
    assert api.option_detector_alerts(dataset_id="fixture", limit=50, offset=0).available
    assert calls[0]["dataset_id"] == "fixture" and calls[0]["scope"] == "LATEST"


def test_eod_evaluation_api_is_read_only_dispatch(monkeypatch):
    from options import api
    from options.analytics import behavior_review

    calls = []
    monkeypatch.setattr(behavior_review, "build_detector_evaluation_review",
        lambda **kwargs: calls.append(kwargs) or dict(storage_ready=False, rows=[], total=0))
    result = api.option_alert_evaluations(dataset_id="fixture", limit=50, offset=0)
    assert result.available and not result.data["storage_ready"]
    assert calls[0]["dataset_id"] == "fixture"


def test_evaluation_reader_validates_canonical_records_and_filters_before_ui(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    record = evaluation_records()[0]
    session = record.package.scheduled_cycle.date()
    cursor = MagicMock()
    cursor.fetchone.return_value = {"ready": True}
    stored = dict(evaluation_id=record.evaluation_id, payload_text=record.canonical_json(), payload_sha256=record.sha256)
    @contextmanager
    def read():
        yield cursor
    repository = OptionAlertEvaluationRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    cursor.fetchall.side_effect = [[dict(dataset_id=record.dataset_id, session_date=session)], [stored], []]
    result = repository.review_inputs(as_of=record.selected_at, session_date=session)
    assert result["records"] == [record]
    for call in cursor.execute.call_args_list:
        assert call.args[0].strip().startswith(("SET", "SELECT"))
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])
    cursor.fetchall.side_effect = [[dict(dataset_id=record.dataset_id, session_date=session)], [{**stored, "payload_sha256": "0" * 64}]]
    with pytest.raises(ValueError, match="identity/hash"):
        repository.review_inputs(as_of=record.selected_at)


def test_dual_origin_selection_rejects_reference_only_and_withholds_incomplete_runs():
    from options.analytics.alert_selection import select_dual_origin_packages

    package = qualified_package()
    kwargs = dict(decision_at=NOW, scheduled_cycle=package.scheduled_cycle,
        expected_underlyers=("AAPL",), completed_matrices={"AAPL": package.matrix_id})
    with pytest.raises(ValueError, match="fully qualified"):
        select_dual_origin_packages([{"status": "CANDIDATE_REFERENCE_NOT_ADMISSION"}], {}, **kwargs)
    assert select_dual_origin_packages([], {}, **kwargs)["members"] == []
    result = select_dual_origin_packages([qualified_package()], {}, **{**kwargs, "completed_matrices": {}})
    assert result["status"] == "INCOMPLETE_UNIVERSE" and result["members"] == []
    result = select_dual_origin_packages([qualified_package()], {}, **{**kwargs, "decision_at": NOW + timedelta(minutes=1)})
    assert result["members"] == []


def qualification_inputs(structure="LONG_CALL", model="O1"):
    from dataclasses import replace
    from decimal import Decimal
    from options.alert_plans import AlertManagementPolicy
    from options.dual_origin import StockFirstPolicy, assess_options_first, assess_stock_first

    inputs, source = package_handoff_inputs(structure)
    inputs["snapshots"] = (replace(inputs["snapshots"][0], day_volume=source.day_volume, open_interest=source.open_interest),
        *inputs["snapshots"][1:])
    setup = inputs["setup"]
    if model == "S2":
        setup = resumption_setup(inputs["setup"])
    from options.dual_origin import StockResumptionPolicy
    policy_type = StockResumptionPolicy if model == "S2" else StockFirstPolicy
    policy = policy_type(source_policy_sha256=setup.source_policy.sha256)
    decision = (assess_options_first(source, inputs["stock"], direction=setup.candidate.direction,
        market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"]) if model == "O1" else
        assess_stock_first(setup, source, policy=policy, trusted_source_policy=setup.source_policy,
            market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"]))
    values = {name: inputs[name] for name in ("candidate", "package_assessment", "stock", "security", "lineage",
        "snapshots", "references", "raw_bars", "raw_bar_created_ats", "source_received_at", "planned_entry_at", "entry_deadline", "valuation_policy")}
    values.update(decision=decision, source=source, setup=setup if model in ("S1", "S2") else None,
        trusted_source_policy=setup.source_policy if model in ("S1", "S2") else None, stock_first_policy=policy if model in ("S1", "S2") else None,
        exit_deadline=inputs["planned_exit_at"], entry_limit=-inputs["candidate"].net_premium,
        management_policy=AlertManagementPolicy("dual_origin_fixture_management_v1", inputs["candidate"].strategy_name,
            Decimal("0.35"), Decimal("0.5"), 7200))
    return values


@pytest.mark.parametrize("model", ["O1", "S1", "S2"])
@pytest.mark.parametrize("structure", ["LONG_CALL", "LONG_PUT", "CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"])
def test_qualification_reuses_raw_basis_and_explicit_plan_before_selection(model, structure):
    import json
    from options.analytics.alert_selection import select_dual_origin_packages
    from options.dual_origin import qualify_dual_origin_package

    inputs = qualification_inputs(structure, model)
    qualified, plan = qualify_dual_origin_package(**inputs)
    payload = json.loads(plan.payload_json)
    assert qualified.plan_sha256 == plan.sha256
    assert qualified.status == "QUALIFIED_INDICATIVE" and qualified.event_horizon_status == "UNAVAILABLE"
    assert payload["version"] == "dual_origin_indicative_plan_v1"
    assert payload["event_horizon"]["complete_options_event_coverage"] is False
    assert payload["fill"] is None and payload["execution_permission"] is False
    assert payload["management_policy"]["stop_loss_fraction"] == "0.35"
    result = select_dual_origin_packages([qualified], {}, decision_at=qualified.decision_at,
        scheduled_cycle=qualified.scheduled_cycle, expected_underlyers=("AAPL",), completed_matrices={"AAPL": qualified.matrix_id})
    assert result["members"] == [qualified]


@pytest.mark.parametrize("mutation", ["raw_price", "adjusted", "late_receipt", "management", "entry_deadline", "exit_expiry", "valuation", "activity", "thesis_source"])
def test_qualification_rejects_unbound_sources_and_incomplete_plans(mutation):
    from dataclasses import replace
    from decimal import Decimal
    from options.dual_origin import qualify_dual_origin_package

    inputs = qualification_inputs()
    if mutation == "raw_price": inputs["raw_bars"] = (replace(inputs["raw_bars"][0], close_price=Decimal("99")),)
    if mutation == "adjusted": inputs["raw_bars"] = (replace(inputs["raw_bars"][0], adjusted=True),)
    if mutation == "late_receipt": inputs["source_received_at"] += timedelta(seconds=1)
    if mutation == "management": inputs["management_policy"] = None
    if mutation == "entry_deadline": inputs["entry_deadline"] = inputs["candidate"].valid_until
    if mutation == "exit_expiry": inputs["exit_deadline"] = inputs["snapshots"][0].expiration_cutoff
    if mutation == "valuation": inputs["valuation_policy"] = inputs["valuation_policy"].model_copy(update={"maximum_source_age_seconds": 1})
    if mutation == "activity": inputs["snapshots"] = (replace(inputs["snapshots"][0], day_volume=1),)
    if mutation == "thesis_source": inputs["stock"] = None
    with pytest.raises(ValueError):
        qualify_dual_origin_package(**inputs)


def test_qualification_blocks_known_events_but_never_promotes_unknown_coverage():
    from options.dual_origin import qualify_dual_origin_package

    inputs = qualification_inputs()
    clock = inputs["decision"].decision_at
    event = dict(source="fixture-calendar", source_key="earnings:AAPL", event_type="EARNINGS",
        affected_underlying="AAPL", first_observed_at=clock - timedelta(minutes=1),
        scheduled_time=clock + timedelta(minutes=30), market_event_id=uuid4(),
        status="SCHEDULED", confidence="CONFIRMED")
    inputs["event_detail"] = dict(event_coverage_evidence=[{"source": "fixture-calendar"}],
        holding_event_evidence=[event])
    with pytest.raises(ValueError, match="known event"):
        qualify_dual_origin_package(**inputs)
    event["first_observed_at"] = clock + timedelta(seconds=1)
    qualified, _ = qualify_dual_origin_package(**inputs)
    assert qualified.event_horizon_status == "UNAVAILABLE" and not qualified.execution_permission


def technical_management_inputs(direction=1, source_kind="STRUCTURE_SNAPSHOT"):
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel

    evidence = TechnicalExitEvidence(security_id=uuid4(), underlyer="AAPL", direction=direction,
        source_kind=source_kind, source_policy_sha256="a" * 64, source_payload_sha256="b" * 64,
        source_revision_ids=(uuid4(),), interval="1h", market_time=MARKET,
        available_at=NOW - timedelta(minutes=1), received_at=NOW, valid_until=NOW + timedelta(minutes=30),
        atr=Decimal("2"), levels=(
            TechnicalLevel(level_id="invalidation", role="INVALIDATION", price=Decimal(100 - direction * 2)),
            TechnicalLevel(level_id="near_structure", role="OPPOSING_STRUCTURE", price=Decimal(100 + direction * 4)),
            TechnicalLevel(level_id="far_structure", role="OPPOSING_STRUCTURE", price=Decimal(100 + direction * 8)),
            TechnicalLevel(level_id="fib", role="FIBONACCI", price=Decimal(100 + direction * 4) + Decimal("0.1"))))
    return dict(evidence=evidence, trusted_source_policy_sha256="a" * 64, security_id=evidence.security_id,
        underlyer="AAPL", direction=direction, stock_entry=Decimal("100"), entry_debit=Decimal("200"),
        decision_at=NOW, exit_deadline=NOW + timedelta(hours=1), session_close=NOW + timedelta(hours=4))


@pytest.mark.parametrize("direction", [1, -1])
def test_technical_management_nearest_structure_and_fibonacci_confluence(direction):
    from options.alert_plans import technical_exit_terms

    arguments = technical_management_inputs(direction)
    result = technical_exit_terms(**arguments)
    assert result["target_level_id"] == "near_structure"
    assert result["fibonacci_confluence"] == ["fib"]
    assert Decimal(result["underlying_stop"]) == Decimal(100 - direction * 2) - direction * Decimal("0.2")
    assert Decimal(result["hard_stop_package_value"]) == 130
    assert Decimal(result["hard_profit_package_value"]) == 300
    setup = technical_exit_terms(**technical_management_inputs(direction, "STOCK_SETUP"))
    assert Decimal(setup["underlying_stop"]) == Decimal(100 - direction * 2)


@pytest.mark.parametrize("mutation", ["late", "wrong_policy", "wrong_identity", "fib_only", "poor_room", "hold"])
def test_technical_management_rejects_missing_or_untrusted_brackets(mutation):
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel, technical_exit_terms

    arguments = technical_management_inputs()
    evidence = arguments["evidence"]
    if mutation == "late": arguments["decision_at"] = NOW - timedelta(seconds=1)
    if mutation == "wrong_policy": arguments["trusted_source_policy_sha256"] = "c" * 64
    if mutation == "wrong_identity": arguments["security_id"] = uuid4()
    if mutation == "fib_only":
        arguments["evidence"] = TechnicalExitEvidence.model_validate({**evidence.model_dump(),
            "levels": tuple(level for level in evidence.levels if level.role != "OPPOSING_STRUCTURE")})
    if mutation == "poor_room":
        arguments["evidence"] = TechnicalExitEvidence.model_validate({**evidence.model_dump(),
            "levels": (*evidence.levels, TechnicalLevel(level_id="obstacle", role="OPPOSING_STRUCTURE", price=Decimal("101")))})
    if mutation == "hold": arguments["exit_deadline"] = NOW + timedelta(hours=3)
    with pytest.raises(ValueError):
        technical_exit_terms(**arguments)


def technical_qualification_inputs(model="O1"):
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel

    inputs = qualification_inputs(model=model)
    decision = inputs["decision"]
    setup = inputs["setup"]
    spot = inputs["snapshots"][0].spot
    evidence = TechnicalExitEvidence(security_id=inputs["security"].security_id, underlyer=decision.underlyer,
        direction=decision.direction, source_kind="STOCK_SETUP" if setup else "STRUCTURE_SNAPSHOT",
        source_policy_sha256=setup.source_policy.sha256 if setup else "a" * 64,
        source_payload_sha256=setup.sha256 if setup else "b" * 64,
        source_revision_ids=tuple(setup.candidate.revision_ids) if setup else (uuid4(),), interval="1h",
        market_time=inputs["lineage"].market_time, available_at=inputs["source_received_at"],
        received_at=inputs["source_received_at"], valid_until=decision.valid_until, atr=Decimal("1"),
        levels=(TechnicalLevel(level_id="stop", role="INVALIDATION", price=Decimal(str(setup.candidate.stop)) if setup else spot - 1),
            TechnicalLevel(level_id="target", role="OPPOSING_STRUCTURE", price=Decimal(str(setup.candidate.target)) if setup else spot + 3)))
    inputs.update(management_policy=None, technical_evidence=evidence, technical_source_policy_sha256=evidence.source_policy_sha256)
    return inputs


@pytest.mark.parametrize("model", ["O1", "S1", "S2"])
def test_technical_package_version_preserves_old_plans_and_new_source_binding(model):
    import json
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel
    from options.dual_origin import qualify_dual_origin_package, load_qualified_package
    from options.analytics.alert_selection import build_selection_evidence, select_dual_origin_packages, load_evaluation_evidence

    inputs = technical_qualification_inputs(model)
    from options.alert_plans import AlertManagementPolicy
    original, old_plan = qualify_dual_origin_package(**{**inputs, "technical_evidence": None,
        "technical_source_policy_sha256": None, "management_policy": AlertManagementPolicy(
            "dual_origin_fixture_management_v1", inputs["candidate"].strategy_name, Decimal("0.35"), Decimal("0.5"), 7200)})
    evidence = inputs["technical_evidence"]
    spot = inputs["snapshots"][0].spot
    package, plan = qualify_dual_origin_package(**inputs)
    assert load_qualified_package(package) == package
    assert original.recurrence_sha256 != package.recurrence_sha256 and original.exposure_sha256 == package.exposure_sha256
    assert json.loads(old_plan.payload_json)["version"] == "dual_origin_indicative_plan_v1"
    payload = json.loads(plan.payload_json)
    assert payload["version"] == "dual_origin_indicative_plan_v2"
    assert len(payload["management_policy"]["technical_exit"]["package_price_scenarios"]) == 12
    selection = select_dual_origin_packages([package], {}, decision_at=package.decision_at,
        scheduled_cycle=package.scheduled_cycle, expected_underlyers=(package.underlyer,), completed_matrices={package.underlyer: package.matrix_id})
    records = build_selection_evidence(selection, {plan.sha256: plan}, dataset_id="technical-fixture-v1", selected_at=package.decision_at)
    assert load_evaluation_evidence(records[0].canonical_json()) == records[0]
    from options.alert_plans import FrozenOptionAlertPlan, _canonical
    from options.analytics.alert_selection import DetectorSelectionEvidence
    altered = json.loads(plan.payload_json)
    altered["management_policy"]["technical_exit"]["underlying_target"] = "999"
    altered_plan = FrozenOptionAlertPlan(_canonical(altered))
    altered_package = type(package).model_validate({**package.model_dump(), "plan_sha256": altered_plan.sha256})
    with pytest.raises(ValueError, match="frozen policy"):
        DetectorSelectionEvidence.model_validate({**records[0].model_dump(), "package": altered_package,
            "plan_payload_text": altered_plan.payload_json})
    if model == "O1":
        updated = TechnicalExitEvidence.model_validate({**evidence.model_dump(), "source_payload_sha256": "c" * 64,
            "levels": (evidence.levels[0], TechnicalLevel(level_id="new-target", role="OPPOSING_STRUCTURE", price=spot + 4))})
        next_package, next_plan = qualify_dual_origin_package(**{**inputs, "technical_evidence": updated})
        assert next_package.recurrence_sha256 == package.recurrence_sha256
        assert next_plan.sha256 != plan.sha256
    from options.alert_plans import TechnicalExitObservation, assess_technical_exit
    observed_at = inputs["planned_entry_at"] + timedelta(seconds=1)
    technical = payload["management_policy"]["technical_exit"]
    observation = TechnicalExitObservation(plan_sha256=plan.sha256, security_id=evidence.security_id,
        market_time=observed_at, recorded_at=observed_at, received_at=observed_at,
        underlying_close=Decimal(technical["underlying_target"]), stock_interval="1h", stock_bar_revision_id=uuid4(),
        package_value=inputs["entry_limit"], leg_snapshot_ids=tuple(uuid4() for _ in inputs["candidate"].legs))
    result = assess_technical_exit(plan, observation, as_of=observed_at, maximum_source_age_seconds=1800)
    assert result["reasons"] == ["TECHNICAL_TARGET"] and result["fill"] is None
    with pytest.raises(ValueError, match="altered policy"):
        assess_technical_exit(altered_plan, observation, as_of=observed_at, maximum_source_age_seconds=1800)
    wrong_plan = TechnicalExitObservation.model_validate({**observation.model_dump(), "plan_sha256": "0" * 64})
    with pytest.raises(ValueError, match="identity or clocks"):
        assess_technical_exit(plan, wrong_plan, as_of=observed_at, maximum_source_age_seconds=1800)
    changed_observation = TechnicalExitObservation.model_validate({**observation.model_dump(),
        "underlying_close": None, "stock_interval": None, "stock_bar_revision_id": None,
        "package_value": inputs["entry_limit"] * Decimal("0.6")})
    result = assess_technical_exit(plan, changed_observation, as_of=observed_at, maximum_source_age_seconds=1800)
    assert result["reasons"] == ["HARD_PREMIUM_STOP"] and result["unavailable_inputs"] == ["TECHNICAL_CLOSE_UNAVAILABLE"]
    missing = TechnicalExitObservation.model_validate({**changed_observation.model_dump(), "package_value": None, "leg_snapshot_ids": ()})
    assert assess_technical_exit(plan, missing, as_of=observed_at, maximum_source_age_seconds=1800)["status"] == "UNAVAILABLE"
    assert assess_technical_exit(plan, missing, as_of=inputs["exit_deadline"], maximum_source_age_seconds=1800)["reasons"] == ["TIME_LIMIT"]


def test_technical_replay_preflight_has_read_only_bounded_original_cutoffs(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
    from scripts.report_stock_behavior_coverage import parse_args

    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = dict(stock_publications=0, imported_by_session_end=0)
    @contextmanager
    def read():
        yield cursor
    repository = OptionStockBehaviorAssessmentRepository()
    monkeypatch.setattr(repository, "_cursor", read)
    result = repository.technical_replay_preflight(configuration=detector_run_inputs()["configuration"], session_date=NOW.date(), as_of=NOW)
    assert result["rows"] == []
    for call in cursor.execute.call_args_list:
        assert call.args[0].strip().startswith(("SET", "SELECT", "WITH"))
        if len(call.args) > 1:
            assert call.args[0].count("%s") == len(call.args[1])
    monkeypatch.setattr("sys.argv", ["report", "--technical-replay-preflight", "2026-09-18"])
    assert parse_args().technical_replay_preflight == NOW.date()
    monkeypatch.setattr("sys.argv", ["report", "--technical-replay-preflight", "2026-09-18", "--detector-sources"])
    with pytest.raises(SystemExit):
        parse_args()


def test_management_version_changes_recurrence_without_changing_exposure():
    from dataclasses import replace
    from options.dual_origin import qualify_dual_origin_package

    inputs = qualification_inputs()
    first, first_plan = qualify_dual_origin_package(**inputs)
    inputs["management_policy"] = replace(inputs["management_policy"], policy_version="fixture_management_v2")
    second, second_plan = qualify_dual_origin_package(**inputs)
    assert first.exposure_sha256 == second.exposure_sha256
    assert first.recurrence_sha256 != second.recurrence_sha256
    assert first_plan.sha256 != second_plan.sha256


def test_forward_selection_rejects_reconstructed_mode_and_wrong_prior_identity():
    from options.analytics.alert_selection import select_dual_origin_packages

    package = qualified_package()
    kwargs = dict(decision_at=NOW, scheduled_cycle=package.scheduled_cycle,
        expected_underlyers=("AAPL",), completed_matrices={"AAPL": package.matrix_id})
    with pytest.raises(ValueError):
        select_dual_origin_packages([package.model_copy(update={"evidence_mode": "RECONSTRUCTED"})], {}, **kwargs)
    prior = qualified_package(99, decision_at=NOW - timedelta(hours=1))
    with pytest.raises(ValueError, match="prior alert identity"):
        select_dual_origin_packages([package], {package.recurrence_sha256: prior}, **kwargs)
    for change in ({"matrix_id": uuid4()}, {"scheduled_cycle": package.scheduled_cycle + timedelta(minutes=15)}):
        with pytest.raises(ValueError, match="exact completed cycle"):
            select_dual_origin_packages([package.model_copy(update=change)], {}, **kwargs)


def resumption_setup(original):
    from dataclasses import replace
    from equity.behavior_setup import DirectResumptionSourcePolicy, bind_direct_stock_setup
    from research.stock_idea_engine import candidate_record, digest
    from test_equity_behavior_setup import publication_inputs

    stock = replace(original.candidate, model="resumption", policy_version="relative_trend_resumption_intraday_v1")
    _, instance, publication, shared = publication_inputs("resumption")
    instance["enrollment"] = [{"security_id": stock.security_id}]
    instance["enrollment_sha256"] = digest(instance["enrollment"])
    instance["instance_id"] = digest(["stock_alert_results_v1", {key: instance[key]
        for key in ("policy_hash", "enrolled_at", "enrollment_sha256")}])
    publication["candidates"] = {stock.episode_id: candidate_record(stock)}
    publication["expected_members"] = [stock.security_id]
    publication["dispositions"][0].update(episode_id=stock.episode_id, security_id=stock.security_id,
        model=stock.model, direction=stock.direction)
    policy = DirectResumptionSourcePolicy(instance_id=instance["instance_id"],
        instance_policy_sha256=instance["policy_hash"], publication_policy_sha256=shared.publication_policy_sha256,
        runtime_sources=shared.runtime_sources)
    return bind_direct_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
        payload_sha256=digest(publication), received_at=original.source.received_at, episode_id=stock.episode_id,
        security_id=original.source.security_id, ticker=stock.ticker, policy=policy)


@pytest.mark.parametrize("mutation", ["wrong_policy", "wrong_family", "expired", "missing_activity", "opposite_type"])
def test_s2_confirmation_keeps_source_contract_and_missingness(mutation):
    from options.dual_origin import StockFirstPolicy, StockResumptionPolicy, assess_stock_first

    inputs, source = package_handoff_inputs()
    setup = resumption_setup(inputs["setup"])
    policy = StockResumptionPolicy(source_policy_sha256=setup.source_policy.sha256)
    clock = inputs["decision_at"]
    if mutation == "wrong_policy": policy = StockFirstPolicy(source_policy_sha256=setup.source_policy.sha256)
    if mutation == "wrong_family": setup = inputs["setup"]
    if mutation == "expired": clock = setup.source.valid_until
    if mutation == "missing_activity": source = None
    if mutation == "opposite_type": source = source.model_copy(update={"contract_type": "PUT"})
    arguments = dict(policy=policy, trusted_source_policy=setup.source_policy, market_cutoff=clock, decision_at=clock)
    if mutation in ("wrong_policy", "wrong_family"):
        with pytest.raises(ValueError):
            assess_stock_first(setup, source, **arguments)
    else:
        result = assess_stock_first(setup, source, **arguments)
        assert result.detector_id == "S2" and result.origin_id == setup.candidate.episode_id
        assert result.disposition == ("UNMATCHED" if mutation == "opposite_type" else "UNAVAILABLE")


def test_s2_versioned_package_can_be_retained_but_o2_cannot_enter_alert_budget():
    from options.analytics.alert_selection import build_selection_evidence, select_dual_origin_packages
    from options.dual_origin import QualifiedDualOriginPackage, SignalDecision, qualify_dual_origin_package
    from options.surface_detection import assess_surface_first

    inputs = qualification_inputs(model="S2")
    package, plan = qualify_dual_origin_package(**inputs)
    assert package.schema_version == "dual_origin_qualified_package_v2"
    with pytest.raises(ValueError):
        QualifiedDualOriginPackage.model_validate_json(package.canonical_json())
    with pytest.raises(ValueError):
        SignalDecision.model_validate_json(inputs["decision"].canonical_json())
    kwargs = dict(decision_at=package.decision_at, scheduled_cycle=package.scheduled_cycle,
        expected_underlyers=("AAPL",), completed_matrices={"AAPL": package.matrix_id})
    selection = select_dual_origin_packages([package], {}, **kwargs)
    records = build_selection_evidence(selection, {plan.sha256: plan}, dataset_id="all-models-fixture", selected_at=package.decision_at)
    assert records[0].package.detector_id == "S2"
    source, stock, surface_context = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=surface_context["decision_at"], decision_at=surface_context["decision_at"])
    with pytest.raises(ValueError, match="fully qualified"):
        select_dual_origin_packages([observation], {}, **kwargs)


def test_o2_evaluation_is_readable_without_fake_candidate_or_package_outcomes():
    from types import SimpleNamespace
    from options.analytics.alert_selection import build_surface_evidence, load_evaluation_evidence
    from options.analytics.behavior_review import build_detector_evaluation_review
    from options.surface_detection import assess_surface_first

    source, stock, inputs = surface_inputs()
    observation = assess_surface_first(source, stock, market_cutoff=inputs["decision_at"], decision_at=inputs["decision_at"])
    records = build_surface_evidence([observation], dataset_id="surface-fixture", selected_at=inputs["decision_at"])
    record = records[0]
    assert record.candidate_id is None and record.selection_status == "OBSERVATION"
    assert load_evaluation_evidence(record.canonical_json()) == record
    repository = SimpleNamespace(review_inputs=lambda **_: dict(ready=True, datasets=[record.dataset_id],
        sessions=[source.scheduled_cycle.date()], dataset_id=record.dataset_id, session_date=source.scheduled_cycle.date(), records=records))
    report = build_detector_evaluation_review(as_of=inputs["decision_at"], detector="O2", repository=repository)
    assert report["total"] == 1 and report["rows"][0]["candidate_id"] is None
    assert report["rows"][0]["direction"] is None and report["rows"][0]["category"] == "NEUTRAL_VOL"
    assert report["cells"][0]["outcome_status"] == "NOT_APPLICABLE_OBSERVATION"
    assert report["cells"][0]["positive_rate"] is None
    with pytest.raises(ValueError, match="once per run"):
        build_surface_evidence([observation, observation], dataset_id="surface-fixture", selected_at=inputs["decision_at"])