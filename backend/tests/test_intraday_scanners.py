"""Deterministic fixtures for the predeclared intraday 30m scanners."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

import pandas as pd
import pytest

from equity.domain import SecurityReferenceRevision
from equity.intraday_research import (
    IntradayScannerEvent,
    build_intraday_event,
    intraday_event_evidence,
    intraday_policy_keys,
    intraday_scanner_outcome_policies,
)
from equity.polygon import sha256_json
from research.intraday_scanners import (
    CONTROL_ORACLE_SOURCE,
    CONTROL_RANDOM_SOURCE,
    CONTROL_SOURCES,
    FAILED_OPENING_RANGE_SOURCE,
    INTRADAY_DETECTOR_POLICY,
    INTRADAY_OUTCOME_HORIZONS,
    INTRADAY_SCANNER_REGISTRY,
    OPENING_RANGE_SOURCE,
    TREND_PULLBACK_SOURCE,
    VWAP_SOURCE,
    benchmark_context,
    build_intraday_scanner_events,
    prepare_intraday_frame,
)


SESSIONS = 25
BARS_PER_SESSION = 13
FIRST_BAR_UTC = timedelta(hours=14, minutes=30)


def _session_dates(count: int = SESSIONS):
    return [value.date() for value in pd.bdate_range("2024-01-02", periods=count)]


def _bar(session_date, slot: int, close: float, *, high: float, low: float,
         volume: float, vwap: float | None) -> dict:
    bar_start = (
        pd.Timestamp(session_date, tz="UTC") + FIRST_BAR_UTC + timedelta(minutes=30 * slot)
    )
    return {
        "bar_revision_id": uuid5(NAMESPACE_URL, f"test-bar:{session_date}:{slot}"),
        "session_date": session_date,
        "bar_start": bar_start,
        "bar_end": bar_start + timedelta(minutes=30),
        "open": close,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": vwap,
    }


def _flat_frame(*, close: float = 100.0, half_range: float = 0.5,
                volume: float = 1000.0) -> pd.DataFrame:
    rows = [
        _bar(session_date, slot, close,
             high=close + half_range, low=close - half_range,
             volume=volume, vwap=close)
        for session_date in _session_dates()
        for slot in range(BARS_PER_SESSION)
    ]
    return pd.DataFrame(rows)


def _ramp_frame(*, slope: float = 0.05, half_range: float = 1.0,
                volume: float = 1000.0) -> pd.DataFrame:
    rows = []
    index = 0
    for session_date in _session_dates():
        for slot in range(BARS_PER_SESSION):
            close = 100.0 + slope * index
            rows.append(_bar(
                session_date, slot, close,
                high=close + half_range, low=close - half_range,
                volume=volume, vwap=close,
            ))
            index += 1
    return pd.DataFrame(rows)


def _position(frame: pd.DataFrame, session_index: int, slot: int) -> int:
    return session_index * BARS_PER_SESSION + slot


def _set(frame: pd.DataFrame, position: int, **values) -> None:
    for key, value in values.items():
        frame.loc[position, key] = value


def _flat_benchmark():
    return benchmark_context("SPY", _flat_frame())


def test_registry_freezes_versions_intervals_and_plan_contracts():
    assert set(INTRADAY_SCANNER_REGISTRY) == {
        OPENING_RANGE_SOURCE, FAILED_OPENING_RANGE_SOURCE,
        VWAP_SOURCE, TREND_PULLBACK_SOURCE,
        CONTROL_ORACLE_SOURCE, CONTROL_RANDOM_SOURCE,
    }
    for registration in INTRADAY_SCANNER_REGISTRY.values():
        assert registration.supported_intervals == ("30m",)
    for name in (OPENING_RANGE_SOURCE, FAILED_OPENING_RANGE_SOURCE,
                 VWAP_SOURCE, TREND_PULLBACK_SOURCE):
        registration = INTRADAY_SCANNER_REGISTRY[name]
        assert registration.is_control is False
        assert registration.required_plan_fields == ("stop_price", "target_price")
    assert set(CONTROL_SOURCES) == {CONTROL_ORACLE_SOURCE, CONTROL_RANDOM_SOURCE}
    for name in CONTROL_SOURCES:
        assert INTRADAY_SCANNER_REGISTRY[name].outcome_modes == ("DIRECTIONAL_HORIZON",)
    assert INTRADAY_OUTCOME_HORIZONS["30m"] == {"30m": 1, "60m": 2, "120m": 4}
    assert INTRADAY_DETECTOR_POLICY["relative_volume_multiple"] == 1.0
    assert INTRADAY_DETECTOR_POLICY["volume_baseline_sessions"] == 20


def test_lookahead_control_takes_the_direction_of_the_bar_it_will_enter_on():
    frame = _ramp_frame()
    # The helper builds doji bars; give each one a body so direction is defined.
    for position in range(len(frame)):
        close = float(frame.loc[position, "close"])
        frame.loc[position, "open"] = close - 0.5 if position % 2 == 0 else close + 0.5
    events, _ = build_intraday_scanner_events(
        "AAPL", frame, scanners=(CONTROL_ORACLE_SOURCE,),
    )
    assert events
    prepared = prepare_intraday_frame(frame)
    by_bar = {row["bar_revision_id"]: row for _, row in prepared.iterrows()}
    for event in events:
        entry = by_bar[UUID(event["metadata"]["entry_bar_id"])]
        expected = 1 if float(entry["close"]) > float(entry["open"]) else -1
        assert event["direction"] == expected


def test_random_control_is_deterministic_and_roughly_balanced():
    frame = _ramp_frame()
    first, _ = build_intraday_scanner_events(
        "AAPL", frame, scanners=(CONTROL_RANDOM_SOURCE,),
    )
    second, _ = build_intraday_scanner_events(
        "AAPL", frame, scanners=(CONTROL_RANDOM_SOURCE,),
    )
    assert [row["direction"] for row in first] == [row["direction"] for row in second]
    assert first
    longs = sum(row["direction"] == 1 for row in first)
    assert 0 < longs < len(first)


def test_controls_only_register_the_directional_return_contract():
    policies = intraday_scanner_outcome_policies(
        effective_from=datetime(2024, 1, 1, tzinfo=timezone.utc),
        source_names=CONTROL_SOURCES,
    )
    assert len(policies) == len(CONTROL_SOURCES)
    assert all(":SIGNED:" in policy.policy_key for policy in policies)


def test_prepare_frame_uses_prior_sessions_only_for_slot_volume_baseline():
    prepared = prepare_intraday_frame(_flat_frame())
    sessions = INTRADAY_DETECTOR_POLICY["volume_baseline_sessions"]
    early = prepared[prepared["session_date"] == _session_dates()[sessions - 1]]
    assert early["slot_volume_baseline"].isna().all()
    late = prepared[prepared["session_date"] == _session_dates()[sessions]]
    assert (late["slot_volume_baseline"] == 1000.0).all()


def test_prepare_frame_marks_session_vwap_unavailable_when_a_bar_lacks_vwap():
    frame = _flat_frame()
    _set(frame, _position(frame, SESSIONS - 1, 1), vwap=None)
    prepared = prepare_intraday_frame(frame)
    tail = prepared.iloc[_position(frame, SESSIONS - 1, 1):]
    last_session = tail[tail["session_date"] == _session_dates()[-1]]
    assert last_session["session_vwap"].isna().all()


def test_opening_range_breakout_emits_one_event_per_episode():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=5000.0)
    _set(frame, _position(frame, last, 2),
         close=103.0, open=103.0, high=103.5, low=102.5, vwap=103.0, volume=5000.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(OPENING_RANGE_SOURCE,),
    )
    assert len(events) == 1
    event = events[0]
    assert event["direction"] == 1
    assert event["scanner_name"] == OPENING_RANGE_SOURCE
    assert event["metadata"]["opening_range_high"] == 100.5
    assert event["stop_price"] < event["entry_price"] < event["target_price"]
    assert event["signal_time"] == frame.loc[_position(frame, last, 1), "bar_end"]


def test_opening_range_breakout_requires_relative_volume_above_slot_median():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=1000.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(OPENING_RANGE_SOURCE,),
    )
    assert events == []


def test_opening_range_breakout_records_missing_benchmark_context():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=5000.0)
    events, diagnostics = build_intraday_scanner_events(
        "AAPL", frame, market=None, sector=None,
        scanners=(OPENING_RANGE_SOURCE,),
    )
    assert events == []
    assert diagnostics["market_context_unavailable"] >= 1


def test_failed_opening_breakout_is_a_separate_reversal_event():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=5000.0)
    _set(frame, _position(frame, last, 2),
         close=99.8, open=99.8, high=100.3, low=99.3, vwap=99.8, volume=5000.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(FAILED_OPENING_RANGE_SOURCE,),
    )
    assert len(events) == 1
    event = events[0]
    assert event["scanner_name"] == FAILED_OPENING_RANGE_SOURCE
    assert event["direction"] == -1
    assert event["metadata"]["break_side"] == "upside"
    assert event["stop_price"] > event["entry_price"] > event["target_price"]


def test_vwap_reclaim_requires_opening_displacement_and_close_location():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 0),
         close=100.0, open=100.0, high=100.5, low=99.5, vwap=101.0)
    _set(frame, _position(frame, last, 1),
         close=100.0, open=100.0, high=100.5, low=99.5, vwap=101.0)
    _set(frame, _position(frame, last, 2),
         close=102.0, open=101.0, high=102.1, low=101.0, vwap=101.0, volume=5000.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(VWAP_SOURCE,),
    )
    assert len(events) == 1
    event = events[0]
    assert event["direction"] == 1
    assert event["metadata"]["trigger_type"] == "vwap_reclaim"
    assert event["metadata"]["opening_displacement_atr"] > 0
    assert event["metadata"]["vwap_policy_version"] == (
        INTRADAY_DETECTOR_POLICY["vwap_policy_version"]
    )


def test_vwap_reclaim_is_unavailable_without_bar_vwap():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 0),
         close=100.0, open=100.0, high=100.5, low=99.5, vwap=None)
    _set(frame, _position(frame, last, 1),
         close=100.0, open=100.0, high=100.5, low=99.5, vwap=101.0)
    _set(frame, _position(frame, last, 2),
         close=102.0, open=101.0, high=102.1, low=101.0, vwap=101.0, volume=5000.0)
    events, diagnostics = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(VWAP_SOURCE,),
    )
    assert events == []
    assert diagnostics["vwap_session_vwap_unavailable"] >= 1


def test_intraday_trend_pullback_requires_contraction_then_resumption():
    frame = _ramp_frame()
    last = SESSIONS - 1
    impulse = _position(frame, last, 6)
    pullback = _position(frame, last, 7)
    resumption = _position(frame, last, 8)
    reference_close = float(frame.loc[impulse, "close"])
    _set(frame, impulse, volume=1500.0)
    pullback_close = reference_close - 0.3
    _set(frame, pullback, close=pullback_close, open=reference_close,
         high=pullback_close + 1.0, low=pullback_close - 1.0,
         vwap=pullback_close, volume=500.0)
    resumption_close = reference_close + 1.0
    _set(frame, resumption, close=resumption_close, open=pullback_close,
         high=resumption_close + 0.2, low=resumption_close - 1.0,
         vwap=resumption_close, volume=1500.0)

    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(TREND_PULLBACK_SOURCE,),
    )
    assert len(events) == 1
    event = events[0]
    assert event["direction"] == 1
    assert event["metadata"]["pullback_bars"] == 1
    assert event["metadata"]["impulse_bar_id"] == str(frame.loc[impulse, "bar_revision_id"])
    assert event["stop_price"] < event["entry_price"] < event["target_price"]


def test_intraday_trend_pullback_rejects_expanding_pullback_volume():
    frame = _ramp_frame()
    last = SESSIONS - 1
    impulse = _position(frame, last, 6)
    pullback = _position(frame, last, 7)
    resumption = _position(frame, last, 8)
    reference_close = float(frame.loc[impulse, "close"])
    _set(frame, impulse, volume=1500.0)
    pullback_close = reference_close - 0.3
    _set(frame, pullback, close=pullback_close, open=reference_close,
         high=pullback_close + 1.0, low=pullback_close - 1.0,
         vwap=pullback_close, volume=4000.0)
    resumption_close = reference_close + 1.0
    _set(frame, resumption, close=resumption_close, open=pullback_close,
         high=resumption_close + 0.2, low=resumption_close - 1.0,
         vwap=resumption_close, volume=1500.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(TREND_PULLBACK_SOURCE,),
    )
    assert events == []


def test_no_scanner_signals_on_the_final_bar_of_a_session():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, BARS_PER_SESSION - 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=5000.0)
    events, _ = build_intraday_scanner_events(
        "AAPL", frame, market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(
            OPENING_RANGE_SOURCE, FAILED_OPENING_RANGE_SOURCE,
            VWAP_SOURCE, TREND_PULLBACK_SOURCE,
        ),
    )
    assert events == []


def test_events_serialize_to_replayable_research_events_and_evidence():
    frame = _flat_frame()
    last = SESSIONS - 1
    _set(frame, _position(frame, last, 1),
         close=102.0, open=102.0, high=102.5, low=101.5, vwap=102.0, volume=5000.0)
    detections, _ = build_intraday_scanner_events(
        "AAPL", frame,
        market=_flat_benchmark(), sector=_flat_benchmark(),
        scanners=(OPENING_RANGE_SOURCE,),
    )
    universe_run_id = uuid5(NAMESPACE_URL, "test-universe-run")
    event = build_intraday_event(
        detection=detections[0],
        universe_run_id=universe_run_id,
        universe_policy_version="liquid_us_common_stocks_v2",
    )
    assert event.interval == "30m"
    assert event.payload["qualification_eligible"] is True
    assert event.payload["research_cohort"] == "FIXED_COHORT_EXPLORATORY"
    round_tripped = IntradayScannerEvent.from_dict(
        json.loads(json.dumps(event.as_dict()))
    )
    assert round_tripped == event

    moment = datetime(2024, 1, 1, tzinfo=timezone.utc)
    security = SecurityReferenceRevision(
        security_revision_id=uuid5(NAMESPACE_URL, "test-security-revision"),
        security_id=uuid5(NAMESPACE_URL, "test-security"),
        ticker="AAPL", active=True, company_name=None, security_type="CS",
        cik=None, composite_figi=None, share_class_figi=None,
        primary_exchange=None, sic_code=None, sic_description=None,
        sector="Information Technology", industry=None, list_date=None,
        delisted_date=None, weighted_shares=None, free_float=None,
        free_float_percent=None, market_cap=Decimal("1"), source="test",
        effective_from=moment, observed_at=moment,
        payload_sha256=sha256_json({}), raw_payload_json="{}",
    )
    evidence = intraday_event_evidence(event, security)
    assert evidence.interval == "30m"
    assert evidence.direction == event.direction
    assert evidence.observed_at == event.signal_time
    assert evidence.market_time == event.signal_time
    assert evidence.quality_state.value == "RESEARCH_ONLY"


def test_outcome_policies_cover_both_declared_return_contracts():
    effective_from = datetime(2024, 1, 1, tzinfo=timezone.utc)
    policies = intraday_scanner_outcome_policies(effective_from=effective_from)
    expected = sum(
        len(registration.outcome_modes)
        for registration in INTRADAY_SCANNER_REGISTRY.values()
    )
    assert len(policies) == expected
    for policy in policies:
        assert policy.interval == "30m"
        assert policy.entry_model == "NEXT_ACTIONABLE_BAR_OPEN_V1"
        assert json.loads(policy.horizons_json) == INTRADAY_OUTCOME_HORIZONS["30m"]
        assert json.loads(policy.benchmark_policy_json)["primary"] == "SECTOR"
    keys = intraday_policy_keys(sorted(INTRADAY_SCANNER_REGISTRY))
    assert set(keys) == {policy.policy_key for policy in policies}


def test_unsupported_interval_is_rejected():
    with pytest.raises(ValueError):
        intraday_scanner_outcome_policies(
            interval="1d", effective_from=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
