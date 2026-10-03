from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from test_canonical_trend import evidence
from test_dual_origin import MARKET, NOW, activity_source


def direct_source(direction_levels):
    return dict(interval="30m", market_time=MARKET, computed_at=NOW - timedelta(seconds=5), setup_direction="BULLISH",
        bar_revision_ids=(uuid4(), uuid4()), levels=direction_levels)


def levels():
    fib = {"levels": [{"name": "50.0%", "price": 100.5}]}
    return {1: {"stops": [{"price": 99.0, "source": "Price Action"}, {"price": 98.0, "source": "Gap"}],
                "targets": [{"price": 103.0, "source": "Gap"}, {"price": 104.0, "source": "ATR"}], "atr": 1.2, "fibonacci": fib},
            -1: {"stops": [{"price": 102.0, "source": "Price Action"}], "targets": [{"price": 97.0, "source": "Gap"}],
                "atr": 1.2, "fibonacci": fib}}


def test_direct_structural_levels_bind_either_direction_with_new_policy():
    from options.detector_collection import bind_direct_structural_levels, direct_structural_policy_sha256

    security = SimpleNamespace(security_id=uuid4(), ticker="AAPL")
    for direction, stop, target in ((1, Decimal("99.0"), Decimal("103.0")), (-1, Decimal("102.0"), Decimal("97.0"))):
        technical = bind_direct_structural_levels(direct_source(levels()), security=security, direction=direction,
            spot=Decimal("100"), received_at=NOW, valid_until=NOW + timedelta(hours=2))
        assert technical.source_policy_sha256 == direct_structural_policy_sha256()
        assert next(level.price for level in technical.levels if level.role == "INVALIDATION") == stop
        assert any(level.role == "OPPOSING_STRUCTURE" and level.price == target for level in technical.levels)
        assert all(level.price != Decimal("104.0") for level in technical.levels)
    with pytest.raises(ValueError):
        bind_direct_structural_levels(direct_source({1: levels()[1]}), security=security, direction=-1,
            spot=Decimal("100"), received_at=NOW, valid_until=NOW + timedelta(hours=2))


def test_direct_structural_levels_require_exact_window_bar():
    from options.detector_collection import compute_direct_structural_levels

    security = SimpleNamespace(security_id=uuid4(), ticker="AAPL")
    stale = SimpleNamespace(bar_end=MARKET - timedelta(minutes=30), session_date=MARKET.date(), bar_revision_id=uuid4())
    repository = SimpleNamespace(list_final_as_of=lambda *args, **kwargs: (stale,))
    with pytest.raises(ValueError, match="window bar unavailable"):
        compute_direct_structural_levels(security=security, interval="30m", window=MARKET, as_of=NOW,
            bar_repository=repository, corporate_action_repository=None)


def test_direct_structural_levels_require_trend_window_revision():
    from options.detector_collection import compute_direct_structural_levels

    security = SimpleNamespace(security_id=uuid4(), ticker="AAPL")
    current = SimpleNamespace(bar_end=MARKET, session_date=MARKET.date(), bar_revision_id=uuid4())
    repository = SimpleNamespace(list_final_as_of=lambda *args, **kwargs: (current,))
    with pytest.raises(ValueError, match="revision differs"):
        compute_direct_structural_levels(security=security, interval="30m", window=MARKET, as_of=NOW,
            bar_repository=repository, corporate_action_repository=None,
            expected_latest_revision_id=uuid4())


def leg(side, contract_type, expiration, contract_id, *, spot=100, delta=0.5, theta=-0.05, vega=0.1):
    return SimpleNamespace(side=SimpleNamespace(value=side), contract_type=SimpleNamespace(value=contract_type),
        expiration_date=expiration, contract_id=contract_id, spot=Decimal(spot), local_iv=0.3, local_delta=delta,
        local_theta_per_day=theta, local_vega_per_vol_point=vega, ratio=1, multiplier=100)


def candidate(strategy, rank, dte, contract_id, contract_type="CALL"):
    session = date(2026, 9, 18)
    legs = (leg("BUY", contract_type, session + timedelta(days=dte), contract_id),)
    if strategy == "DIRECTIONAL_DEBIT_SPREAD":
        legs += (leg("SELL", contract_type, session + timedelta(days=dte), contract_id + 1000),)
    return SimpleNamespace(underlyer="AAPL", strategy_name=strategy, structure_type=SimpleNamespace(value="LONG_CALL"),
        legs=legs, rank=rank, candidate_id=uuid4(), net_premium=Decimal("-2.5"), maximum_loss=Decimal("250"),
        maximum_profit=None, breakevens=(Decimal("102.5"),))


def test_matched_expressions_keep_best_candidate_per_strategy_and_dte_bucket():
    from options.intraday_participation import matched_expressions

    pool = [candidate("DIRECTIONAL_LONG_PREMIUM", 5, 3, 1), candidate("DIRECTIONAL_LONG_PREMIUM", 2, 5, 2),
        candidate("DIRECTIONAL_LONG_PREMIUM", 4, 30, 3), candidate("DIRECTIONAL_DEBIT_SPREAD", 1, 14, 4),
        candidate("DIRECTIONAL_LONG_PREMIUM", 1, 30, 5, contract_type="PUT")]
    rows = matched_expressions(pool, underlyer="AAPL", direction=1, session_date=date(2026, 9, 18),
        activity_contract_ids={2}, rv20=0.25)
    assert [(row.strategy_name, row.dte_bucket, row.candidate_rank) for row in rows] == [
        ("DIRECTIONAL_DEBIT_SPREAD", "08-21", 1), ("DIRECTIONAL_LONG_PREMIUM", "01-07", 2),
        ("DIRECTIONAL_LONG_PREMIUM", "22-45", 4)]
    near = rows[1]
    assert near.is_activity_contract and near.iv_over_rv20 == pytest.approx(1.2)
    assert near.expected_move == pytest.approx(100 * 0.3 * (5 / 365) ** 0.5)


def test_matched_expressions_keep_missing_greeks_observation_only():
    from options.intraday_participation import matched_expressions

    item = candidate("DIRECTIONAL_LONG_PREMIUM", 1, 5, 1)
    item.legs = (leg("BUY", "CALL", date(2026, 9, 23), 1, delta=None, theta=None, vega=None),)
    row, = matched_expressions((item,), underlyer="AAPL", direction=1, session_date=date(2026, 9, 18),
        activity_contract_ids=set(), rv20=0.25)
    assert row.long_delta is None
    assert row.theta_per_maximum_loss is None and row.vega_per_maximum_loss is None


def test_opening_lane_measures_gap_and_held_range_break():
    from options.intraday_participation import opening_measurements

    session = date(2026, 9, 18)
    session_open = datetime(2026, 9, 18, 13, 30, tzinfo=timezone.utc)
    rows = [dict(interval="15m", session_date=session, bar_end=session_open + timedelta(minutes=15),
        high_price=101, low_price=99, open_price=100, close_price=100.5)]
    for index, close in enumerate((100.2, 100.8, 101.4, 101.6), start=1):
        rows.append(dict(interval="5m", session_date=session, bar_end=session_open + timedelta(minutes=15 + 5 * index),
            open_price=100.4 if index == 1 else close, high_price=close + 0.1, low_price=close - 0.1, close_price=close))
    values = opening_measurements(rows, session_date=session, session_open=session_open,
        decision_at=session_open + timedelta(minutes=45), direction=1, prior_close=99.0)
    assert values["gap_open_return"] == pytest.approx(100.4 / 99 - 1)
    assert values["opening_range_break_held"] == 1.0 and values["minutes_since_open"] == 45
    assert "opening_range_break_held" not in opening_measurements(rows, session_date=session,
        session_open=session_open, decision_at=session_open, direction=-1, prior_close=99.0)
    future = dict(rows[-1], bar_end=session_open + timedelta(minutes=50), close_price=98)
    causal = opening_measurements((*rows, future), session_date=session, session_open=session_open,
        decision_at=session_open + timedelta(minutes=45), direction=1, prior_close=99.0)
    assert causal["opening_range_break_held"] == 1.0
    missing = [row for row in rows if row.get("bar_end") != session_open + timedelta(minutes=30)]
    assert "opening_range_break_held" not in opening_measurements(missing, session_date=session,
        session_open=session_open, decision_at=session_open + timedelta(minutes=45), direction=1, prior_close=99.0)


def test_o1_v3_observation_adds_opening_dte_and_expressions_without_admission():
    from options.analytics.alert_selection import build_o1_indicator_evidence, load_evaluation_evidence
    from options.dual_origin import assess_options_canonical_trend
    from options.intraday_participation import O1IndicatorObservationV3, build_o1_indicator_observation_v3, matched_expressions

    source = activity_source()
    stock = evidence(source)
    decision = assess_options_canonical_trend(source, stock, direction=1, market_cutoff=MARKET, decision_at=NOW)
    expressions = matched_expressions([candidate("DIRECTIONAL_LONG_PREMIUM", 1, 30, 9)], underlyer="AAPL", direction=1,
        session_date=source.volume_session, activity_contract_ids={123}, rv20=0.2)
    observation = build_o1_indicator_observation_v3(source, decision, stock, scheduled_cycle=MARKET,
        regime={"SPY": 0.1, "QQQ": 0.2}, prior_same_direction=False,
        extras=dict(minutes_since_open=30, gap_open_return=0.01, opening_range_break_held=1.0,
            spy_session_return=0.004, qqq_session_return=0.005, activity_dte=28, activity_iv=0.18, rv20_cc_annual=0.2),
        expressions=expressions)
    assert isinstance(observation, O1IndicatorObservationV3) and observation.policy.changes_admission is False
    verdicts = {row.challenger_id: row.verdict for row in observation.challengers}
    assert verdicts["BASELINE_V4"] == "PASS" and verdicts["OPENING_LANE_RANGE_BREAK_HELD"] == "PASS"
    assert verdicts["ACTIVITY_DTE_8_PLUS"] == "PASS" and verdicts["ACTIVITY_IV_BELOW_RV20"] == "PASS"
    assert len(observation.matched_expressions) == 1
    record, = build_o1_indicator_evidence((observation,), dataset_id="v36-fixture", selected_at=NOW)
    assert load_evaluation_evidence(record.canonical_json()) == record


def test_direct_structure_launch_v9_pins_policies_and_v8_still_validates():
    from options.detector_collection import direct_structural_policy_sha256
    from options.detector_launch import (
        CANONICAL_TREND_RUNTIME_FILES, DIRECT_STRUCTURE_RUNTIME_FILES, CanonicalTrendDetectorForwardLaunch,
        DirectStructureDetectorForwardLaunch, _source_hashes, decode_detector_forward_launch,
    )
    from options.intraday_participation import (
        CANONICAL_TREND_ALIGNMENT_POLICY, CANONICAL_TREND_SOURCE_POLICY, O1_INDICATOR_REVIEW_POLICY_V2,
        O1_INDICATOR_REVIEW_POLICY_V3,
    )
    from options.stock_bar_detection import STOCK_BAR_SIGNAL_POLICY_SHA256

    backend = Path(__file__).resolve().parents[2]
    common = dict(dataset_id="v36-fixture", effective_from=NOW, underlyers=("AAPL",), configuration_sha256="a" * 64,
        strategy_policy_sha256="b" * 64, valuation_policy_sha256="c" * 64, stock_bar_policy_sha256=STOCK_BAR_SIGNAL_POLICY_SHA256,
        o1_confirmation_policy_sha256=CANONICAL_TREND_ALIGNMENT_POLICY.sha256,
        o1_trend_source_policy_sha256=CANONICAL_TREND_SOURCE_POLICY.sha256)
    launch = DirectStructureDetectorForwardLaunch(**common,
        o1_indicator_review_policy_sha256=O1_INDICATOR_REVIEW_POLICY_V3.sha256,
        structural_level_policy_sha256=direct_structural_policy_sha256(),
        runtime_sources=_source_hashes(backend, DIRECT_STRUCTURE_RUNTIME_FILES))
    assert decode_detector_forward_launch(launch.canonical_json()) == launch
    from options.detector_launch import PartialCoverageDetectorForwardLaunch

    partial_launch = PartialCoverageDetectorForwardLaunch.model_validate({**launch.model_dump(),
        "schema_version": "option_detector_forward_launch_v10"})
    assert decode_detector_forward_launch(partial_launch.canonical_json()) == partial_launch
    assert partial_launch.coverage_policy == "PER_UNDERLYER_COMPLETE_CURRENT_MATRIX_V1"
    for changes in ({"structural_level_policy_sha256": "0" * 64},
                    {"o1_indicator_review_policy_sha256": O1_INDICATOR_REVIEW_POLICY_V2.sha256}):
        with pytest.raises(ValueError):
            DirectStructureDetectorForwardLaunch.model_validate({**launch.model_dump(), **changes})
    v8 = CanonicalTrendDetectorForwardLaunch(**common, o1_indicator_review_policy_sha256=O1_INDICATOR_REVIEW_POLICY_V2.sha256,
        runtime_sources=_source_hashes(backend, CANONICAL_TREND_RUNTIME_FILES))
    assert decode_detector_forward_launch(v8.canonical_json()) == v8


def test_o1_indicator_review_v3_migration_matches_baseline():
    backend = Path(__file__).resolve().parents[2]
    sql = (backend / "migrations" / "061_option_o1_indicator_review_v3.sql").read_text(encoding="utf-8")
    baseline = (backend / "migrations" / "000_canonical_schema.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline
    for version in ("v1", "v2", "v3"):
        assert f"option_participation_indicator_review_{version}" in sql
    assert "observation->'policy'->'execution_permission'='false'::jsonb" in sql
    assert "observation->'publication_permission'='false'::jsonb" in sql
    assert "ALTER TABLE" not in sql and "DROP" not in sql


def test_partial_detector_run_requires_exact_unavailable_ticker_coverage():
    from options.analytics.alert_selection import PartialDetectorRunEvidence, load_detector_run

    run = PartialDetectorRunEvidence(dataset_id="partial-fixture", scheduled_cycle=MARKET,
        selected_at=NOW, configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64,
        market_policy_sha256="c" * 64, strategy_version="fixture",
        expected_underlyers=("AAPL", "SOFI"), source_matrices=(("AAPL", uuid4()),),
        market_time=MARKET, observed_time=NOW, record_sha256s=(),
        selection_counts=(("NOT_SELECTED", 0), ("OBSERVATION", 0), ("REPEAT", 0), ("SELECTED", 0)),
        unavailable_underlyers=(("SOFI", ("DATA_QUALITY_GATE_FAILED",)),))
    assert load_detector_run(run.canonical_json()) == run
    assert run.coverage_status == "PARTIAL"
    for unavailable in ((), (("AAPL", ("DATA_QUALITY_GATE_FAILED",)),), (("SOFI", ()),)):
        with pytest.raises(ValueError):
            PartialDetectorRunEvidence.model_validate({**run.model_dump(), "unavailable_underlyers": unavailable})


def test_partial_collector_retains_configured_universe_and_healthy_sources():
    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs

    matrix = dict(underlying="AAPL", matrix_id=uuid4(), market_time=MARKET, observed_time=NOW)
    configuration = SimpleNamespace(settings=SimpleNamespace(underlyers=("AAPL", "SOFI")),
        configuration_sha256="a" * 64, strategy_policy_sha256="b" * 64, policy_sha256="c" * 64,
        strategy_policy=SimpleNamespace(strategy_version="fixture", forward_admission=True))
    repository = SimpleNamespace(prior_selected=lambda **_: {})
    reader = lambda **_: DetectorCycleInputs(matrices=(matrix,))
    collector = DetectorCycleCollector(reader, repository, clock=lambda: NOW, partial_coverage=True)
    arguments = dict(configuration=configuration, dataset_id="partial-fixture", scheduled_cycle=MARKET,
        completed_matrices={"AAPL": matrix["matrix_id"]}, started_at=NOW,
        unavailable_underlyers=(("SOFI", ("DATA_QUALITY_GATE_FAILED",)),))
    run, records = collector(**arguments)
    assert records == () and run.coverage_status == "PARTIAL"
    assert run.expected_underlyers == ("AAPL", "SOFI")
    assert run.source_matrices == (("AAPL", matrix["matrix_id"]),)
    legacy = DetectorCycleCollector(reader, repository, clock=lambda: NOW)
    with pytest.raises(ValueError, match="complete causal"):
        legacy(**arguments)


def test_partial_coverage_migration_is_in_baseline_and_keeps_legacy_complete():
    backend = Path(__file__).resolve().parents[2]
    migration = (backend / "migrations/062_option_detector_partial_coverage.sql").read_text(encoding="utf-8")
    baseline = (backend / "migrations/000_canonical_schema.sql").read_text(encoding="utf-8")
    assert migration.strip() in baseline
    assert "selector_version <> 'option_detector_run_v3'" in migration
    assert "AND covered_underlying_count = expected_underlying_count" in migration
    assert "unavailable_underlyers" in migration
    assert "UPDATE " not in migration and "DELETE " not in migration
