from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from test_dual_origin import MARKET, NOW, activity_source


def trend_rows(*, end, count=202, step=0.05, session=None):
    rows = []
    for index in range(count):
        bar_end = end - timedelta(minutes=30 * (count - 1 - index))
        close = 100 + step * index
        rows.append(dict(bar_revision_id=uuid4(), bar_end=bar_end,
            session_date=session if bar_end.date() == end.date() and session else bar_end.date(),
            high_price=close + 0.2, low_price=close - 0.2, close_price=close, volume=1000,
            created_at=bar_end + timedelta(seconds=30), system_observed_at=bar_end + timedelta(seconds=20)))
    return rows


def daily_rows(session, count=22):
    return [dict(bar_revision_id=uuid4(), bar_end=datetime.combine(session - timedelta(days=count - index),
            datetime.min.time(), timezone.utc) + timedelta(hours=20),
        session_date=session - timedelta(days=count - index), high_price=101, low_price=99, close_price=100,
        volume=1_000_000, created_at=NOW - timedelta(days=1), system_observed_at=NOW - timedelta(days=1))
        for index in range(count)]


def evidence(source, *, window=None, step=0.05, split_clear=True, trend=None):
    from options.intraday_participation import build_canonical_trend_evidence, latest_completed_30m_window

    window = latest_completed_30m_window(source.market_time) if window is None else window
    return build_canonical_trend_evidence(security_id=source.security_id, underlyer=source.underlyer,
        session_date=source.volume_session, required_window=window,
        trend_bars=trend if trend is not None else trend_rows(end=window, step=step, session=source.volume_session),
        daily_bars=daily_rows(source.volume_session), split_clear=split_clear,
        received_at=NOW - timedelta(seconds=30), session_close=NOW + timedelta(hours=4))


def test_canonical_trend_uses_exact_current_window_and_reproduces_behavior_formula():
    from equity.behavior_calculators import calculate_trend_metrics

    source = activity_source()
    stock = evidence(source)
    assert stock.status == "READY" and stock.required_window == stock.trend_market_time
    assert stock.trend_bar_count == 200 and stock.daily_bar_count == 21 and len(stock.slope_history) == 3
    rows = trend_rows(end=stock.required_window, session=source.volume_session)[-200:]
    expected = calculate_trend_metrics(*([float(row[key]) for row in rows]
        for key in ("high_price", "low_price", "close_price")), "30m")[0].value
    assert stock.ema50_slope10_atr == pytest.approx(expected) and stock.ema50_slope10_atr > 0
    assert type(stock).model_validate_json(stock.canonical_json()) == stock


@pytest.mark.parametrize(("mutation", "reason"), [
    ("opening", "OPENING_WINDOW_NO_COMPLETED_30M"),
    ("stale", "EXACT_CURRENT_30M_UNAVAILABLE"),
    ("short", "INSUFFICIENT_30M_HISTORY"),
    ("split", "SPLIT_COVERAGE_UNAVAILABLE_OR_CROSSED"),
])
def test_canonical_trend_never_carries_prior_session_or_stale_windows(mutation, reason):
    from options.intraday_participation import latest_completed_30m_window

    source = activity_source()
    window = latest_completed_30m_window(source.market_time)
    if mutation == "opening":
        from options.intraday_participation import build_canonical_trend_evidence

        stock = build_canonical_trend_evidence(security_id=source.security_id, underlyer=source.underlyer,
            session_date=source.volume_session, required_window=None,
            trend_bars=trend_rows(end=window - timedelta(hours=18)), daily_bars=daily_rows(source.volume_session),
            split_clear=True, received_at=NOW - timedelta(seconds=30), session_close=NOW + timedelta(hours=4))
    elif mutation == "stale":
        stock = evidence(source, trend=trend_rows(end=window - timedelta(minutes=30), session=source.volume_session))
    elif mutation == "short":
        stock = evidence(source, trend=trend_rows(end=window, count=150, session=source.volume_session))
    else:
        stock = evidence(source, split_clear=False)
    assert stock.status == "UNAVAILABLE" and reason in stock.reason_codes


def test_canonical_trend_decision_confirms_contradicts_and_requalifies():
    from options.dual_origin import (
        CanonicalTrendSignalDecision, assess_options_canonical_trend, assess_options_credit_canonical_trend,
        load_signal_decision,
    )

    source = activity_source()
    rising = evidence(source)
    decision = assess_options_canonical_trend(source, rising, direction=1, market_cutoff=MARKET, decision_at=NOW)
    assert isinstance(decision, CanonicalTrendSignalDecision) and decision.disposition == "CONFIRMED"
    assert {gate.gate_id for gate in decision.gates} == {"TREND_SLOPE_30m", "UNDERLYING_LIQUIDITY_EVIDENCE"}
    assert load_signal_decision(decision) == decision
    falling = evidence(source, step=-0.05)
    contradicted = assess_options_canonical_trend(source, falling, direction=1, market_cutoff=MARKET, decision_at=NOW)
    assert contradicted.disposition == "CONTRADICTED" and "STOCK_DIRECTION_NOT_ALIGNED" in contradicted.reasons
    put_credit = assess_options_credit_canonical_trend(activity_source(contract_type="PUT", security_id=source.security_id),
        rising, direction=1, market_cutoff=MARKET, decision_at=NOW)
    assert put_credit.disposition == "CONFIRMED"
    missing = assess_options_canonical_trend(source, None, direction=1, market_cutoff=MARKET, decision_at=NOW)
    assert missing.disposition == "UNAVAILABLE" and "CANONICAL_TREND_EVIDENCE_UNAVAILABLE" in missing.reasons
    with pytest.raises(ValueError):
        CanonicalTrendSignalDecision.model_validate({**decision.model_dump(), "gates": ()})


def test_canonical_trend_rejects_evidence_for_another_window():
    from options.dual_origin import assess_options_canonical_trend
    from options.intraday_participation import latest_completed_30m_window

    source = activity_source()
    earlier = latest_completed_30m_window(source.market_time) - timedelta(minutes=30)
    stock = evidence(source, window=earlier)
    decision = assess_options_canonical_trend(source, stock, direction=1, market_cutoff=MARKET, decision_at=NOW)
    assert decision.disposition == "UNAVAILABLE" and "STOCK_WINDOW_MISMATCH" in decision.reasons


def test_o1_v2_observation_records_observation_only_challengers_and_loads_in_envelope():
    from options.analytics.alert_selection import build_o1_indicator_evidence, load_evaluation_evidence
    from options.dual_origin import assess_options_canonical_trend
    from options.intraday_participation import O1IndicatorObservationV2, build_o1_indicator_observation_v2

    source = activity_source()
    stock = evidence(source, step=-0.05)
    decision = assess_options_canonical_trend(source, stock, direction=1, market_cutoff=MARKET, decision_at=NOW)
    observation = build_o1_indicator_observation_v2(source, decision, stock, scheduled_cycle=MARKET,
        regime={"SPY": 0.1, "QQQ": -0.1}, prior_same_direction=True)
    assert isinstance(observation, O1IndicatorObservationV2)
    verdicts = {row.challenger_id: row.verdict for row in observation.challengers}
    assert verdicts["BASELINE_V4"] == "FAIL"
    assert verdicts["MARKET_REGIME_ALIGNED"] == "FAIL"
    for challenger in ("TREND_PULLBACK_LOCATION", "TREND_FLIP_HELD_2_BARS", "REBOUND_LANE_STRUCTURAL",
                       "REBOUND_LANE_WITH_REGIME"):
        assert challenger in verdicts
    assert observation.policy.changes_admission is False
    record, = build_o1_indicator_evidence((observation,), dataset_id="v35-fixture", selected_at=NOW)
    assert load_evaluation_evidence(record.canonical_json()) == record
    assert type(record).model_validate_json(record.canonical_json()).observation == observation


def test_canonical_trend_launch_v8_pins_policies_and_older_launch_still_decodes():
    from options.detector_launch import (
        CANONICAL_STOCK_RUNTIME_FILES, CANONICAL_TREND_RUNTIME_FILES, CanonicalStockFirstDetectorForwardLaunch,
        CanonicalTrendDetectorForwardLaunch, _source_hashes, decode_detector_forward_launch,
    )
    from options.intraday_participation import (
        CANONICAL_TREND_ALIGNMENT_POLICY, CANONICAL_TREND_SOURCE_POLICY, INTRADAY_ALIGNMENT_POLICY,
        O1_INDICATOR_REVIEW_POLICY_V2,
    )
    from options.stock_bar_detection import STOCK_BAR_SIGNAL_POLICY_SHA256

    backend = Path(__file__).resolve().parents[2]
    common = dict(dataset_id="v35-fixture", effective_from=NOW, underlyers=("AAPL",), configuration_sha256="a" * 64,
        strategy_policy_sha256="b" * 64, valuation_policy_sha256="c" * 64,
        stock_bar_policy_sha256=STOCK_BAR_SIGNAL_POLICY_SHA256)
    launch = CanonicalTrendDetectorForwardLaunch(**common,
        o1_confirmation_policy_sha256=CANONICAL_TREND_ALIGNMENT_POLICY.sha256,
        o1_trend_source_policy_sha256=CANONICAL_TREND_SOURCE_POLICY.sha256,
        o1_indicator_review_policy_sha256=O1_INDICATOR_REVIEW_POLICY_V2.sha256,
        runtime_sources=_source_hashes(backend, CANONICAL_TREND_RUNTIME_FILES))
    assert decode_detector_forward_launch(launch.canonical_json()) == launch
    assert "equity/behavior_calculators.py" in dict(launch.runtime_sources)
    for changes in ({"o1_confirmation_policy_sha256": INTRADAY_ALIGNMENT_POLICY.sha256},
                    {"o1_trend_source_policy_sha256": "0" * 64},
                    {"runtime_sources": launch.runtime_sources[:-1]}):
        with pytest.raises(ValueError):
            CanonicalTrendDetectorForwardLaunch.model_validate({**launch.model_dump(), **changes})
    older = CanonicalStockFirstDetectorForwardLaunch(**common,
        o1_confirmation_policy_sha256=INTRADAY_ALIGNMENT_POLICY.sha256,
        runtime_sources=_source_hashes(backend, CANONICAL_STOCK_RUNTIME_FILES))
    assert decode_detector_forward_launch(older.canonical_json()) == older


def test_s1_s2_accept_derived_hourly_lineage_marker_only():
    from options.stock_bar_detection import CanonicalStockBarPoint

    values = dict(bar_revision_id=uuid4(), interval="1h", session_date=date(2026, 9, 18),
        bar_start=MARKET - timedelta(hours=1), bar_end=MARKET, open=100, high=101, low=99, close=100.5,
        system_observed_at=MARKET + timedelta(minutes=1), created_at=MARKET + timedelta(minutes=2),
        payload_sha256="d" * 64, quality_codes=("DERIVED_FROM_CANONICAL_30M",))
    assert CanonicalStockBarPoint(**values).quality_codes == ("DERIVED_FROM_CANONICAL_30M",)
    with pytest.raises(ValueError):
        CanonicalStockBarPoint(**{**values, "quality_codes": ("PARTIAL_SESSION",)})
    with pytest.raises(ValueError):
        CanonicalStockBarPoint(**{**values, "interval": "30m"})


def test_o1_indicator_review_v2_migration_accepts_both_versions_and_matches_baseline():
    backend = Path(__file__).resolve().parents[2]
    sql = (backend / "migrations" / "060_option_o1_indicator_review_v2.sql").read_text(encoding="utf-8")
    baseline = (backend / "migrations" / "000_canonical_schema.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline
    assert "option_participation_indicator_review_v1" in sql and "option_participation_indicator_review_v2" in sql
    assert "changes_admission'='false'::jsonb" in sql
    assert "ALTER TABLE" not in sql and "DROP" not in sql
