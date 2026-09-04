import json
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityBarRevision,
    EvidenceRole,
    EvidenceType,
    SecurityReferenceRevision,
)
from equity.materialization import (
    PortalStrategyResults,
    _portal_strategy_results,
    derive_session_hourly_frame,
    materialize_equity_evidence,
)
from equity.setup_composition import compose_fibonacci_context


UTC = timezone.utc
HASH = "a" * 64


def security():
    return SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=uuid4(), ticker="AAPL",
        active=True, company_name="Apple Inc.", security_type="CS",
        cik="0000320193", composite_figi="BBG000B9XRY4",
        share_class_figi="BBG001S5N8V8", primary_exchange="XNAS",
        sic_code="3571", sic_description="ELECTRONIC COMPUTERS",
        sector="Information Technology", industry="Technology Hardware",
        list_date=date(1980, 12, 12), delisted_date=None,
        weighted_shares=Decimal("15000000000"),
        free_float=Decimal("14500000000"), free_float_percent=96.67,
        market_cap=Decimal("2800000000000"),
        source="POLYGON_TICKER_OVERVIEW_V3",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        payload_sha256=HASH, raw_payload_json="{}",
    )


def bars(count=220, *, final=True, interval="30m"):
    interval_minutes = {
        "15m": 15, "30m": 30, "1h": 60, "1d": 24 * 60, "1wk": 7 * 24 * 60,
    }[interval]
    start = datetime(2026, 8, 20, 13, 30, tzinfo=UTC)
    results = []
    for index in range(count):
        bar_start = start + timedelta(minutes=interval_minutes * index)
        close = Decimal(str(100 + index * 0.05))
        results.append(EquityBarRevision(
            bar_revision_id=uuid4(), security_id=SECURITY.security_id,
            ticker="AAPL", interval=interval, session_date=bar_start.date(),
            bar_start=bar_start, bar_end=bar_start + timedelta(minutes=interval_minutes),
            open_price=close - Decimal("0.1"), high_price=close + Decimal("0.3"),
            low_price=close - Decimal("0.3"), close_price=close,
            volume=Decimal(str(1000 + index * 10)), vwap=close,
            transaction_count=100, source_kind=BarSourceKind.NATIVE_REST,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
            is_final=final,
            system_observed_at=bar_start + timedelta(minutes=interval_minutes),
            replay_available_at=None, adjusted=False, payload_sha256=HASH,
        ))
    return tuple(results)


def test_weekly_portal_strategy_bundle_skips_session_opening_gaps():
    frame = pd.DataFrame({
        "open": np.linspace(100, 120, 220),
        "high": np.linspace(101, 121, 220),
        "low": np.linspace(99, 119, 220),
        "close": np.linspace(100.5, 120.5, 220),
        "volume": np.full(220, 1_000_000),
    })

    with patch("equity.materialization.scan_gap_strategies") as gap_scanner:
        result = _portal_strategy_results("AAPL", frame, "1wk")

    assert result.gaps == ()
    gap_scanner.assert_not_called()


SECURITY = security()


def session_bars(session_date, count, *, start_hour, start_minute=30):
    start = datetime(
        session_date.year, session_date.month, session_date.day,
        start_hour, start_minute, tzinfo=UTC,
    )
    results = []
    for index in range(count):
        bar_start = start + timedelta(minutes=30 * index)
        close = Decimal(str(100 + index))
        results.append(EquityBarRevision(
            bar_revision_id=uuid4(), security_id=SECURITY.security_id,
            ticker="AAPL", interval="30m", session_date=session_date,
            bar_start=bar_start, bar_end=bar_start + timedelta(minutes=30),
            open_price=close - Decimal("0.5"), high_price=close + Decimal("1"),
            low_price=close - Decimal("1"), close_price=close,
            volume=Decimal("100"), vwap=close, transaction_count=10,
            source_kind=BarSourceKind.NATIVE_REST,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
            system_observed_at=bar_start + timedelta(minutes=30),
            replay_available_at=None, adjusted=False, payload_sha256=HASH,
        ))
    return tuple(results)


def test_session_hourly_derivation_includes_regular_close_partial():
    source_bars = session_bars(date(2026, 8, 28), 13, start_hour=13)

    frame, source_ids = derive_session_hourly_frame(source_bars)

    assert len(frame) == 7
    assert frame.index[-1] == pd.Timestamp("2026-08-28T19:30:00Z")
    assert frame["close"].tolist() == [101.0, 103.0, 105.0, 107.0, 109.0, 111.0, 112.0]
    assert frame["volume"].tolist() == [200.0] * 6 + [100.0]
    assert source_ids == tuple(row.bar_revision_id for row in source_bars)


def test_session_hourly_derivation_requires_complete_bucket():
    source_bars = session_bars(date(2026, 8, 28), 2, start_hour=13)

    frame, source_ids = derive_session_hourly_frame(source_bars[:1])

    assert frame.empty
    assert source_ids == ()


def test_session_hourly_derivation_includes_early_close_partial():
    source_bars = session_bars(date(2026, 11, 27), 7, start_hour=14)

    frame, source_ids = derive_session_hourly_frame(source_bars)

    assert len(frame) == 4
    assert frame.index[-1] == pd.Timestamp("2026-11-27T17:30:00Z")
    assert frame["close"].tolist() == [101.0, 103.0, 105.0, 106.0]
    assert len(source_ids) == 7


def scanner_frame(latest_time):
    return pd.DataFrame([{
        "scanner_name": "breakout_expansion",
        "scanner_version": "1.0",
        "ticker": "AAPL",
        "date": latest_time,
        "direction": -1,
        "trigger_type": "swing_breakout",
        "setup_anchor": "anchor-1",
        "entry_price": 110.0,
        "atr_at_signal": 2.0,
        "reference_level": 111.0,
        "stop_price": 112.0,
        "target_price": 106.0,
        "metadata": json.dumps({"volume_ratio": 1.5}),
    }])


def test_materializer_emits_common_evidence_families_and_conflict():
    source_bars = bars()
    latest = source_bars[-1]
    with (
        patch(
            "equity.materialization.build_all_scanner_events",
            return_value=scanner_frame(latest.bar_start),
        ),
        patch(
            "equity.materialization.detect_forming_patterns",
            return_value=[{
                "type": "BEAR_FLAG", "bias": "BEARISH", "status": "FORMING",
                "readiness": "AT_EDGE", "edge_distance_atr": 0.2,
                "start_time": int(latest.bar_start.timestamp()),
            }],
        ) as pattern_detector,
        patch(
            "equity.materialization.detect_price_channel",
            return_value={
                "type": "RISING_CHANNEL", "direction": "BULLISH",
                "start_time": latest.bar_start.isoformat(),
            },
        ),
    ):
        result = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="30m",
            bars=source_bars, observed_at=latest.bar_end + timedelta(seconds=2),
            fundamental_metrics={
                "free_float": Decimal("14500000000"),
                "market_cap": Decimal("2800000000000"),
                "debt_to_equity": 1.2,
            },
            fundamental_report_ids=(uuid4(),),
        )

    types = {row.evidence_type for row in result.evidence}
    assert {
        EvidenceType.FEATURE_SNAPSHOT,
        EvidenceType.SCANNER_RESULT,
        EvidenceType.PATTERN_OBSERVATION,
        EvidenceType.PRICE_CHANNEL,
        EvidenceType.TRADE_SETUP,
    }.issubset(types)
    assert result.ema_direction == "BULLISH"
    assert result.setup_direction == "BULLISH"
    scanner = next(
        row for row in result.evidence
        if row.evidence_type is EvidenceType.SCANNER_RESULT
    )
    assert scanner.qualification_revision_id is None
    assert "UNQUALIFIED_DIRECTION" in scanner.quality_codes
    feature = next(
        row for row in result.evidence
        if row.evidence_type is EvidenceType.FEATURE_SNAPSHOT
    )
    assert json.loads(feature.payload_json)["float_turnover"] > 0
    setup = next(
        row for row in result.evidence
        if row.evidence_type is EvidenceType.TRADE_SETUP
    )
    setup_payload = json.loads(setup.payload_json)
    assert setup_payload["setup_policy_version"] == "equity_setup_v13"
    assert len(setup_payload["setup_policy_sha256"]) == 64
    assert setup_payload["ticker"] == "AAPL"
    assert setup_payload["interval"] == "30m"
    assert setup_payload["last_close"] == float(latest.close_price)
    assert setup_payload["technicals"]["ema8"] is not None
    assert setup_payload["ema_alignment"]["confirm_interval"] == "1h"
    assert setup_payload["ema_alignment"]["confirm"] == "Bullish"
    assert setup_payload["ema_alignment"]["confirm_ema8"] is not None
    confirmation = next(
        row for row in result.evidence if row.source_name == "EMA_CONFIRMATION"
    )
    assert confirmation.evidence_type is EvidenceType.REGIME_SIGNAL
    assert confirmation.evidence_role is EvidenceRole.REGIME
    assert confirmation.interval == "1h"
    assert confirmation.direction == 1
    assert confirmation.evidence_id in setup.source_revision_ids
    assert setup_payload["momentum"]["state"]
    assert "candlestick_patterns" in setup_payload
    assert "golden_cross" in setup_payload
    assert setup_payload["direction"]["bias"] == "Bullish"
    assert setup_payload["signals"]
    assert "strategy_results" in setup_payload
    assert "entries" in setup_payload
    assert setup_payload["targets"]
    assert setup_payload["stops"]
    assert "structural_patterns" in setup_payload
    assert set(setup_payload["timing"]) == {"urgency", "detail"}
    assert set(setup_payload["duration"]) == {"estimate", "detail"}
    assert set(setup_payload["confluence"]) == {"grade", "count"}
    close = setup_payload["last_close"]
    assert all(target["price"] > close for target in setup_payload["targets"])
    assert all(stop["price"] < close for stop in setup_payload["stops"])
    portal_rows = [
        row for row in result.evidence
        if row.source_version == "portal_strategy_bundle_v3"
    ]
    assert portal_rows
    assert all(row.quality_state.value == "RESEARCH_ONLY" for row in portal_rows)
    pattern_detector.assert_called_once()
    assert pattern_detector.call_args.kwargs["input_includes_forming_bar"] is False


def test_materialization_is_deterministic_for_same_input_and_run():
    source_bars = bars(60)
    run_id = uuid4()
    observed_at = source_bars[-1].bar_end + timedelta(seconds=1)
    with (
        patch("equity.materialization.build_all_scanner_events", return_value=pd.DataFrame()),
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
    ):
        first = materialize_equity_evidence(
            analysis_run_id=run_id, security=SECURITY, interval="30m",
            bars=source_bars, observed_at=observed_at,
        )
        repeated = materialize_equity_evidence(
            analysis_run_id=run_id, security=SECURITY, interval="30m",
            bars=source_bars, observed_at=observed_at,
        )

    assert [row.evidence_id for row in first.evidence] == [
        row.evidence_id for row in repeated.evidence
    ]
    assert [row.payload_sha256 for row in first.evidence] == [
        row.payload_sha256 for row in repeated.evidence
    ]


def test_evidence_identity_is_owned_by_run_and_exact_source_lineage():
    source_bars = bars(60)
    run_id = uuid4()
    observed_at = source_bars[-1].bar_end + timedelta(seconds=1)
    changed_source = (
        replace(source_bars[0], bar_revision_id=uuid4()),
        *source_bars[1:],
    )
    with (
        patch("equity.materialization.build_all_scanner_events", return_value=pd.DataFrame()),
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
    ):
        original = materialize_equity_evidence(
            analysis_run_id=run_id, security=SECURITY, interval="30m",
            bars=source_bars, observed_at=observed_at,
        )
        another_run = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="30m",
            bars=source_bars, observed_at=observed_at,
        )
        another_source = materialize_equity_evidence(
            analysis_run_id=run_id, security=SECURITY, interval="30m",
            bars=changed_source, observed_at=observed_at,
        )

    original_ids = {row.evidence_id for row in original.evidence}
    assert original_ids.isdisjoint(row.evidence_id for row in another_run.evidence)
    assert original_ids.isdisjoint(row.evidence_id for row in another_source.evidence)


def test_15m_materialization_emits_only_trigger_and_location_families():
    source_bars = bars(60, interval="15m")
    latest = source_bars[-1]
    with (
        patch("equity.materialization.build_all_scanner_events") as scanners,
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
    ):
        result = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="15m",
            bars=source_bars, observed_at=latest.bar_end + timedelta(seconds=1),
            fundamental_metrics={"market_cap": Decimal("2800000000000")},
        )

    assert {row.evidence_type for row in result.evidence} == {
        EvidenceType.FEATURE_SNAPSHOT,
    }
    assert result.setup_direction == "UNAVAILABLE"
    scanners.assert_not_called()


def test_30m_portal_strategy_inputs_persist_once_before_setup_composition():
    source_bars = bars(220)
    latest = source_bars[-1]
    strategies = PortalStrategyResults(
        gaps=({
            "gap_type": "Support Gap", "gap_low": 108.0, "gap_high": 109.0,
            "last_close": 110.0,
        },),
        fair_value_gaps=({
            "fvg_type": "Bullish FVG", "status": "Unmitigated",
            "fvg_low": 107.0, "fvg_high": 108.0,
        },),
        moving_average={
            "signal": "Above MA", "ma_spread_pct": 1.5,
            "days_since_cross": 10, "weekly_signal": None, "markers": [],
        },
        momentum_pullback={"grade": "A", "score": 8},
        bearish_bounce=None,
    )
    with (
        patch("equity.materialization.build_all_scanner_events", return_value=pd.DataFrame()),
        patch("equity.materialization._portal_strategy_results", return_value=strategies),
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
    ):
        result = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="30m",
            bars=source_bars, observed_at=latest.bar_end + timedelta(seconds=1),
        )

    portal_rows = {
        row.source_name: row for row in result.evidence
        if row.source_version == "portal_strategy_bundle_v3"
    }
    assert set(portal_rows) == {
        "GAP_STRATEGIES", "FAIR_VALUE_GAPS", "MOVING_AVERAGE_CROSSOVER",
        "MOMENTUM_PULLBACK",
    }
    assert portal_rows["GAP_STRATEGIES"].direction is None
    assert portal_rows["FAIR_VALUE_GAPS"].direction is None
    assert portal_rows["MOVING_AVERAGE_CROSSOVER"].direction == 1
    assert portal_rows["MOMENTUM_PULLBACK"].direction == 1
    setup = next(
        row for row in result.evidence
        if row.evidence_type is EvidenceType.TRADE_SETUP
    )
    payload = json.loads(setup.payload_json)
    directional_ids = {
        str(portal_rows["MOVING_AVERAGE_CROSSOVER"].evidence_id),
        str(portal_rows["MOMENTUM_PULLBACK"].evidence_id),
    }
    assert directional_ids.issubset(payload["scanner_evidence_ids"])
    assert payload["strategy_results"]["gaps"]["support_count"] == 1
    assert payload["strategy_results"]["fvg"]["bull_unmitigated"] == 1


def test_30m_fibonacci_is_persisted_once_and_consumed_by_setup():
    source_bars = bars(220)
    latest = source_bars[-1]
    fibonacci = {
        "signal": "Near 61.8% retracement",
        "trend_direction": "uptrend_retracement",
        "swing_basis": "structural_confirmed_leg",
        "swing_detection_pct": 1.5,
        "scope_bars": 126,
        "swing_high": 120.0,
        "swing_low": 100.0,
        "swing_high_date": "2026-08-20",
        "swing_low_date": "2026-08-18",
        "swing_size_pct": 20.0,
        "developing_pivot": {"type": "low", "price": 108.0},
        "active_leg": {
            "levels": [{"name": "38.2%", "price": 112.36}],
        },
        "confirmed_legs": [],
        "nearest_level": "61.8%",
        "nearest_level_price": 107.64,
        "distance_pct": 0.2,
        "retracement_pct": 61.8,
        "progress_reached_pct": 61.8,
        "progress_current_pct": 60.0,
        "target_kind": "support",
        "retracement_levels": [{"name": "61.8%", "price": 107.64}],
        "extension_levels": [],
        "target_levels": [{"name": "61.8%", "price": 107.64}],
        "support_targets": [{"level": "61.8%", "price": 107.64}],
        "resistance_targets": [],
        "fib_236": 115.28,
        "fib_382": 112.36,
        "fib_500": 110.0,
        "fib_618": 107.64,
        "fib_786": 104.28,
    }
    with (
        patch("equity.materialization.build_all_scanner_events", return_value=pd.DataFrame()),
        patch("equity.materialization.scan_gap_strategies", return_value=[]),
        patch("equity.materialization.scan_fair_value_gaps", return_value=[]),
        patch("equity.materialization.scan_moving_average_crossover", return_value=None),
        patch("equity.materialization.scan_momentum_pullback", return_value=None),
        patch("equity.materialization.scan_bearish_bounce", return_value=None),
        patch("equity.materialization.calculate_fibonacci_swing_pct", return_value=1.5),
        patch("equity.materialization.scan_fibonacci", return_value=fibonacci) as fib_scanner,
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
        patch(
            "equity.materialization.analyze_price_structures",
            return_value={"patterns": [], "volume_pivot_zones": []},
        ) as structure_analyzer,
    ):
        result = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="30m",
            bars=source_bars, observed_at=latest.bar_end + timedelta(seconds=1),
        )

    fib_scanner.assert_called_once()
    assert fib_scanner.call_args.kwargs["min_swing_pct"] == 1.5
    fibonacci_row = next(row for row in result.evidence if row.source_name == "FIBONACCI")
    assert fibonacci_row.source_version == "portal_strategy_bundle_v3"
    assert fibonacci_row.evidence_role is EvidenceRole.LOCATION
    assert fibonacci_row.direction is None
    assert fibonacci_row.quality_codes == ("LOCATION_ONLY",)
    assert json.loads(fibonacci_row.payload_json) == fibonacci
    assert structure_analyzer.call_args.args[1] == [
        {"name": "61.8%", "price": 107.64},
        {"name": "38.2%", "price": 112.36},
    ]
    setup = next(
        row for row in result.evidence
        if row.evidence_type is EvidenceType.TRADE_SETUP
    )
    assert fibonacci_row.evidence_id in setup.source_revision_ids
    setup_payload = json.loads(setup.payload_json)
    assert setup_payload["strategy_results"]["fibonacci"] == (
        compose_fibonacci_context(fibonacci)
    )


def test_hourly_setup_consumes_persisted_daily_confirmation_bars():
    source_bars = bars(220, interval="1h")
    confirmation_bars = bars(60, interval="1d")
    latest = source_bars[-1]
    with (
        patch("equity.materialization.build_all_scanner_events", return_value=pd.DataFrame()),
        patch(
            "equity.materialization._portal_strategy_results",
            return_value=PortalStrategyResults((), (), None, None, None),
        ),
        patch("equity.materialization.detect_forming_patterns", return_value=[]),
        patch("equity.materialization.detect_price_channel", return_value=None),
    ):
        result = materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="1h",
            bars=source_bars, confirmation_bars=confirmation_bars,
            observed_at=max(latest.bar_end, confirmation_bars[-1].bar_end) + timedelta(seconds=1),
        )

    confirmation = next(
        row for row in result.evidence if row.source_name == "EMA_CONFIRMATION"
    )
    setup = next(
        row for row in result.evidence if row.evidence_type is EvidenceType.TRADE_SETUP
    )
    payload = json.loads(setup.payload_json)
    assert confirmation.interval == "1d"
    assert confirmation.source_version == "ema_confirmation_1d_persisted_v1"
    assert confirmation.source_revision_ids == tuple(
        row.bar_revision_id for row in confirmation_bars
    )
    assert payload["ema_alignment"]["confirm"] == "Bullish"
    assert confirmation.evidence_id in setup.source_revision_ids


def test_materialization_rejects_latest_unfinalized_bar():
    source_bars = bars(60, final=False)

    with pytest.raises(ValueError, match="latest finalized bar"):
        materialize_equity_evidence(
            analysis_run_id=uuid4(), security=SECURITY, interval="30m",
            bars=source_bars,
            observed_at=source_bars[-1].bar_end + timedelta(seconds=1),
        )