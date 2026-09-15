from datetime import date, datetime, timezone
from dataclasses import replace
from types import SimpleNamespace
import json
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import pytest

from research.historical_signal_replay import (
    BearishBounceAdapter,
    BUILTIN_ADAPTERS,
    CompositeScannersDailyAdapter,
    GapBreakawayConfirmationAdapter,
    GapEntryFillAdapter,
    GapFormationAdapter,
    GapFormationV2Adapter,
    HistoricalSignalContext,
    MovingAverageCrossoverAdapter,
    MomentumPullbackAdapter,
    PatternBoundaryBreakDailyAdapter,
    evaluate_historical_signals,
)


UTC = timezone.utc


def gap_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=21)
    frame = pd.DataFrame({
        "open": [100.5] * 20 + [104.0],
        "high": [101.0] * 20 + [105.0],
        "low": [100.0] * 20 + [103.5],
        "close": [100.5] * 20 + [104.5],
        "volume": [1_000_000] * 20 + [3_000_000],
        "bar_revision_id": [uuid4() for _ in range(21)],
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=[value.date() for value in dates])
    return frame


def append_bar(frame: pd.DataFrame, **values) -> pd.DataFrame:
    next_date = pd.bdate_range(start=pd.Timestamp(frame.index[-1]), periods=2)[-1].date()
    row = {
        "open": values["open"], "high": values["high"],
        "low": values["low"], "close": values["close"],
        "volume": values.get("volume", 1_000_000),
        "bar_revision_id": uuid4(),
        "bar_end": datetime.combine(next_date, datetime.min.time(), tzinfo=UTC),
    }
    return pd.concat((frame, pd.DataFrame([row], index=[next_date])))


def test_gap_adapter_emits_deterministic_formation_event() -> None:
    frame = gap_frame()
    signal_date = frame.index[-1]
    context = HistoricalSignalContext(
        signal_date=signal_date,
        signal_time=frame.iloc[-1]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v1",
        corporate_actions=(),
    )
    adapter = GapFormationAdapter()

    first = adapter.evaluate("AAPL", frame, context)[0]
    second = adapter.evaluate("AAPL", frame, context)[0]

    assert first.event_id == second.event_id
    assert first.direction == 1
    assert first.source_name == "GAP_BREAKAWAY_HOLD"
    assert first.payload["hypothesis"] == "FORMATION_HOLD"
    assert first.payload["gap_classification"] == "BREAKAWAY"
    assert first.source_bar_revision_ids == (frame.iloc[-1]["bar_revision_id"],)
    summary = adapter.summarize((first,))
    assert summary["hypothesis_counts"] == {"FORMATION_HOLD": 1}
    assert summary["extreme_gap_counts"]["at_least_100_pct"] == 0


def test_strict_daily_replay_retains_denominator_and_versions_inputs():
    import exchange_calendars
    from research.historical_signal_replay import evaluate_strict_daily_signals

    calendar = exchange_calendars.get_calendar("XNYS")
    frame = gap_frame()
    sessions = calendar.sessions_in_range("2026-07-30", "2026-08-27")
    assert len(sessions) == len(frame)
    frame.index = [session.date() for session in sessions]
    identity = uuid4()
    cutoff = datetime(2026, 9, 12, tzinfo=UTC)
    frame["security_id"] = identity
    frame["ticker"] = "AAPL"
    frame["bar_start"] = [calendar.session_open(session) for session in sessions]
    frame["bar_end"] = [calendar.session_close(session) for session in sessions]
    frame["replay_available_at"] = frame["bar_end"]
    frame["system_observed_at"] = cutoff
    frame["created_at"] = cutoff
    frame["payload_sha256"] = "a" * 64
    members = {session: frozenset({"AAPL", "MISSING"}) for session in frame.index}
    arguments = dict(members_by_session=members, universe_ids_by_session={session: uuid4() for session in members},
                     universe_policy_version="test", actions_by_session_ticker={
                         (frame.index[0], "AAPL"): ({"action_type": "DIVIDEND", "effective_date": frame.index[0], "security_id": identity},)
                     }, source_cutoff=cutoff,
                     member_security_ids={(session, ticker): identity for session, tickers in members.items() for ticker in tickers})
    result = evaluate_strict_daily_signals(GapFormationV2Adapter(), {"AAPL": frame}, **arguments)
    assert len(result.coverage) == len(members) * 2
    assert sum(row["state"] == "MISSING_SIGNAL_BAR" for row in result.coverage) == len(members)
    assert len(result.events) == 1
    assert result.events[0].payload["security_id"] == str(identity)
    revised = frame.copy()
    revised.iloc[0, revised.columns.get_loc("bar_revision_id")] = uuid4()
    assert evaluate_strict_daily_signals(GapFormationV2Adapter(), {"AAPL": revised}, **arguments).events[0].event_id != result.events[0].event_id
    broken = frame.drop(frame.index[10])
    assert not evaluate_strict_daily_signals(GapFormationV2Adapter(), {"AAPL": broken}, **arguments).events
    mismatched = frame.copy()
    mismatched.iloc[-1, mismatched.columns.get_loc("security_id")] = uuid4()
    rejected = evaluate_strict_daily_signals(GapFormationV2Adapter(), {"AAPL": mismatched}, **arguments)
    assert not rejected.events
    assert any(row["state"] == "IDENTITY_MISMATCH" for row in rejected.coverage)


def test_generic_runner_excludes_split_date_before_adapter_evaluation() -> None:
    frame = gap_frame()
    signal_date = frame.index[-1]
    universe_run_id = uuid4()

    result = evaluate_historical_signals(
        GapFormationAdapter(),
        {"AAPL": frame},
        members_by_session={signal_date: frozenset(("AAPL",))},
        universe_ids_by_session={signal_date: universe_run_id},
        universe_policy_version="policy-v1",
        actions_by_session_ticker={
            (signal_date, "AAPL"): ({"action_type": "SPLIT"},)
        },
    )

    assert result.events == ()
    assert result.exclusion_counts["CORPORATE_ACTION"] == 1


def test_security_replay_uses_own_history_and_own_actions_for_reused_ticker():
    frame = gap_frame()
    healthpeak, physicians = uuid4(), uuid4()
    frame["security_id"] = healthpeak
    frame["source_ticker"] = ["PEAK"] * 20 + ["DOC"]
    arguments = dict(
        members_by_session={frame.index[-1]: frozenset({"DOC"})},
        universe_ids_by_session={frame.index[-1]: uuid4()}, universe_policy_version="test",
        actions_by_session_ticker={}, frames_by_security={healthpeak: frame},
        member_security_ids={(frame.index[-1], "DOC"): healthpeak},
        actions_by_session_security={(frame.index[-1], physicians): ({"action_type": "MERGER"},)},
        price_manifest_sha256="a" * 64,
        action_manifest_sha256="c" * 64,
    )
    result = evaluate_historical_signals(GapFormationAdapter(), {}, **arguments)
    assert len(result.events) == 1
    event = result.events[0]
    assert event.ticker == "DOC"
    assert event.payload["security_id"] == str(healthpeak)
    assert event.source_bar_revision_ids == tuple(frame["bar_revision_id"])
    assert event.payload["price_manifest_sha256"] == "a" * 64
    assert event.payload["action_manifest_sha256"] == "c" * 64
    assert evaluate_historical_signals(GapFormationAdapter(), {}, **arguments).events == result.events
    arguments["action_manifest_sha256"] = "d" * 64
    assert evaluate_historical_signals(GapFormationAdapter(), {}, **arguments).events[0].event_id != event.event_id
    arguments["action_manifest_sha256"] = "c" * 64
    arguments["price_manifest_sha256"] = "b" * 64
    assert evaluate_historical_signals(GapFormationAdapter(), {}, **arguments).events[0].event_id != event.event_id
    arguments["actions_by_session_security"][(frame.index[-1], healthpeak)] = ({"action_type": "SYMBOL_CHANGE"},)
    assert evaluate_historical_signals(GapFormationAdapter(), {}, **arguments).exclusion_counts["CORPORATE_ACTION"] == 1


@pytest.mark.parametrize("failure", ["mixed_frame", "missing_member", "missing_frame", "missing_date", "duplicate_date", "mixed_mode"])
def test_security_replay_rejects_incomplete_or_mixed_identity(failure):
    frame = gap_frame()
    identity = uuid4()
    frame["security_id"] = identity
    arguments = dict(members_by_session={frame.index[-1]: frozenset({"DOC"})},
                     universe_ids_by_session={frame.index[-1]: uuid4()}, universe_policy_version="test",
                     actions_by_session_ticker={}, frames_by_security={identity: frame},
                     member_security_ids={(frame.index[-1], "DOC"): identity},
                     actions_by_session_security={}, price_manifest_sha256="a" * 64, action_manifest_sha256="b" * 64)
    if failure == "mixed_frame":
        frame.iloc[0, frame.columns.get_loc("security_id")] = uuid4()
    elif failure == "missing_member":
        arguments["member_security_ids"] = {}
    elif failure == "missing_frame":
        arguments["frames_by_security"] = {}
    elif failure == "missing_date":
        arguments["frames_by_security"] = {identity: frame.iloc[:-1]}
    elif failure == "duplicate_date":
        arguments["frames_by_security"] = {identity: pd.concat([frame, frame.iloc[-1:]])}
    else:
        arguments["actions_by_session_ticker"] = {(frame.index[-1], "DOC"): ()}
    with pytest.raises(ValueError):
        evaluate_historical_signals(GapFormationAdapter(), {}, **arguments)


def test_security_aware_events_cannot_enter_legacy_outcome_persistence_or_evaluation():
    from scripts import run_historical_signal_outcomes as outcomes

    events = (SimpleNamespace(payload={"price_manifest_sha256": "a" * 64}),)
    with patch.object(outcomes, "EquityUniverseRepository") as repository:
        with pytest.raises(ValueError, match="version-aware outcome prices"):
            outcomes.persist_evidence(events)
        repository.assert_not_called()
    with pytest.raises(ValueError, match="version-aware outcome prices"):
        outcomes.evaluate(events, horizon_sessions=[21], round_trip_cost_bps=4,
                          available_by=datetime(2026, 9, 12, tzinfo=UTC))


def test_security_replay_truncates_prices_and_future_action_context():
    frame = gap_frame()
    identity = uuid4()
    frame["security_id"] = identity
    signal_date = frame.index[-1]
    arguments = dict(members_by_session={signal_date: frozenset({"DOC"})},
                     universe_ids_by_session={signal_date: uuid4()}, universe_policy_version="test",
                     actions_by_session_ticker={}, member_security_ids={(signal_date, "DOC"): identity},
                     actions_by_session_security={}, price_manifest_sha256="a" * 64, action_manifest_sha256="b" * 64)
    original = evaluate_historical_signals(GapFormationV2Adapter(), {}, frames_by_security={identity: frame}, **arguments)
    extended = append_bar(frame, open=1000, high=1002, low=999, close=1001)
    extended["security_id"] = identity
    arguments["actions_by_session_security"] = {(extended.index[-1], identity): ({"action_type": "MERGER"},)}
    adapter = GapFormationV2Adapter()
    with patch.object(adapter, "evaluate", wraps=adapter.evaluate) as evaluate:
        repeated = evaluate_historical_signals(adapter, {}, frames_by_security={identity: extended}, **arguments)
    assert original.events
    assert repeated.events == original.events
    assert evaluate.call_args.args[1].index[-1] == signal_date
    assert evaluate.call_args.args[2].corporate_actions_by_date == {}


def test_gap_adapter_requires_immediately_previous_trading_session() -> None:
    frame = gap_frame().drop(index=gap_frame().index[-2])

    assert GapFormationAdapter().candidate_dates(frame) == ()


def test_v2_gap_adapter_is_stable_when_older_history_is_added() -> None:
    short_frame = gap_frame()
    prefix = short_frame.iloc[:20].copy()
    prefix.index = [value.date() for value in pd.bdate_range(
        end=pd.Timestamp(short_frame.index[0]) - pd.offsets.BDay(), periods=20
    )]
    long_frame = pd.concat((prefix, short_frame))
    signal_date = short_frame.index[-1]
    universe_run_id = uuid4()
    arguments = {
        "members_by_session": {signal_date: frozenset(("AAPL",))},
        "universe_ids_by_session": {signal_date: universe_run_id},
        "universe_policy_version": "policy-v1",
        "actions_by_session_ticker": {},
    }

    short_result = evaluate_historical_signals(
        GapFormationV2Adapter(), {"AAPL": short_frame}, **arguments
    )
    long_result = evaluate_historical_signals(
        GapFormationV2Adapter(), {"AAPL": long_frame}, **arguments
    )

    assert short_result.events == long_result.events
    assert short_result.events[0].source_version == "gap_formation_v2"


def test_breakaway_confirmation_waits_for_close_beyond_formation_high() -> None:
    frame = append_bar(
        gap_frame(), open=104.5, high=107.0, low=104.0, close=106.0
    )
    signal_date = frame.index[-1]

    result = evaluate_historical_signals(
        GapBreakawayConfirmationAdapter(),
        {"AAPL": frame},
        members_by_session={signal_date: frozenset(("AAPL",))},
        universe_ids_by_session={signal_date: uuid4()},
        universe_policy_version="policy-v1",
        actions_by_session_ticker={},
    )

    assert len(result.events) == 1
    event = result.events[0]
    assert event.source_name == "GAP_BREAKAWAY_CONFIRMATION"
    assert event.direction == 1
    assert event.payload["confirmation_sessions"] == 1
    assert event.payload["confirmation_trigger"] == 105.0
    assert event.payload["last_close"] == 106.0
    assert event.payload["stop_price"] == 103.5
    assert event.payload["target_price"] == 111.0


def test_gap_entry_fill_emits_first_unfilled_gap_entry_from_above() -> None:
    frame = append_bar(
        gap_frame(), open=104.5, high=105.0, low=103.7, close=104.0
    )
    frame = append_bar(frame, open=103.8, high=104.0, low=102.0, close=102.5)
    signal_date = frame.index[-1]

    result = evaluate_historical_signals(
        GapEntryFillAdapter(),
        {"AAPL": frame},
        members_by_session={signal_date: frozenset(("AAPL",))},
        universe_ids_by_session={signal_date: uuid4()},
        universe_policy_version="policy-v1",
        actions_by_session_ticker={},
    )

    assert len(result.events) == 1
    event = result.events[0]
    assert event.source_name == "GAP_ENTRY_FILL"
    assert event.direction == -1
    assert event.payload["gap_age_sessions_at_entry"] == 2
    assert event.payload["last_close"] == 102.5
    assert event.payload["stop_price"] == 103.5
    assert event.payload["target_price"] == 101.0
    assert event.payload["target_kind"] == "RANGE_GAP_FAR_EDGE"


def test_gap_entry_fill_excludes_action_between_formation_and_entry() -> None:
    frame = append_bar(
        gap_frame(), open=104.5, high=105.0, low=103.7, close=104.0
    )
    action_date = frame.index[-1]
    frame = append_bar(frame, open=103.8, high=104.0, low=102.0, close=102.5)
    signal_date = frame.index[-1]

    result = evaluate_historical_signals(
        GapEntryFillAdapter(),
        {"AAPL": frame},
        members_by_session={signal_date: frozenset(("AAPL",))},
        universe_ids_by_session={signal_date: uuid4()},
        universe_policy_version="policy-v1",
        actions_by_session_ticker={
            (action_date, "AAPL"): ({"action_type": "SPLIT"},),
        },
    )

    assert result.events == ()


def test_ma_crossover_emits_only_fresh_bullish_sign_change() -> None:
    dates = pd.bdate_range("2024-01-02", periods=22)
    closes = [100.0] * 21 + [110.0]
    frame = pd.DataFrame({
        "open": closes, "high": [value + 1 for value in closes],
        "low": [value - 1 for value in closes], "close": closes,
        "volume": [1_000_000] * 22,
        "bar_revision_id": [uuid4() for _ in dates],
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=[value.date() for value in dates])
    signal_date = frame.index[-1]

    result = evaluate_historical_signals(
        MovingAverageCrossoverAdapter(), {"AAPL": frame},
        members_by_session={signal_date: frozenset(("AAPL",))},
        universe_ids_by_session={signal_date: uuid4()},
        universe_policy_version="policy-v1",
        actions_by_session_ticker={},
    )

    assert len(result.events) == 1
    event = result.events[0]
    assert event.direction == 1
    assert event.source_name == "MA_CROSSOVER_9_21"
    assert event.payload["signal"] == "BULLISH_CROSSOVER"
    assert event.payload["short_ma"] > event.payload["long_ma"]


def test_product_pullback_adapters_are_bounded_and_directional() -> None:
    assert MomentumPullbackAdapter.minimum_bars == 210
    assert MomentumPullbackAdapter.maximum_bars == 211
    assert MomentumPullbackAdapter.direction == 1
    assert MomentumPullbackAdapter.source_name == "MOMENTUM_PULLBACK"
    assert BearishBounceAdapter.minimum_bars == 210
    assert BearishBounceAdapter.maximum_bars == 211
    assert BearishBounceAdapter.direction == -1
    assert BearishBounceAdapter.source_name == "BEARISH_BOUNCE"


def test_composite_adapter_translates_registered_plan_with_bar_lineage() -> None:
    dates = pd.bdate_range("2025-01-02", periods=210)
    bar_ids = [uuid4() for _ in dates]
    frame = pd.DataFrame({
        "ticker": ["AAPL"] * len(dates),
        "open": [100.0] * len(dates),
        "high": [101.0] * len(dates),
        "low": [99.0] * len(dates),
        "close": [100.0] * len(dates),
        "volume": [1_000_000] * len(dates),
        "bar_revision_id": bar_ids,
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=pd.Index([value.date() for value in dates], name="session_date"))
    signal_date = frame.index[-1]
    detector_rows = pd.DataFrame([{
        "scanner_name": "breakout_expansion",
        "scanner_version": "1.0",
        "ticker": "AAPL",
        "date": signal_date,
        "direction": 1,
        "trigger_type": "swing_breakout",
        "setup_anchor": "1:pivot:101",
        "entry_price": 102.0,
        "atr_at_signal": 2.0,
        "reference_level": 101.0,
        "stop_price": 100.0,
        "target_price": 106.0,
        "metadata": json.dumps({"volume_ratio": 1.5}),
    }])
    context = HistoricalSignalContext(
        signal_date=signal_date,
        signal_time=frame.iloc[-1]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v2",
        corporate_actions=(),
    )
    adapter = CompositeScannersDailyAdapter()

    with patch(
        "research.historical_signal_replay.build_all_scanner_events",
        return_value=detector_rows,
    ) as detector:
        assert adapter.candidate_dates(frame) == (signal_date,)
        event = adapter.evaluate("AAPL", frame, context)[0]

    assert detector.call_count == 1
    assert BUILTIN_ADAPTERS["composite-scanners-1d-v1"].source_version == (
        "composite_scanners_daily_v1"
    )
    assert event.source_name == "breakout_expansion"
    assert event.source_version == "1.0"
    assert event.setup_anchor == "AAPL:1:pivot:101"
    assert event.source_bar_revision_ids == (bar_ids[-1],)
    assert event.payload["signal_reference_price"] == 102.0
    assert event.payload["stop_price"] == 100.0
    assert event.payload["target_price"] == 106.0
    assert event.payload["outcome_modes"] == [
        "DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN",
    ]


def test_pattern_break_uses_prior_boundary_and_later_confirmation_bar() -> None:
    dates = pd.bdate_range("2026-01-02", periods=31)
    bar_ids = [uuid4() for _ in dates]
    closes = [99.0] * 30 + [101.0]
    frame = pd.DataFrame({
        "ticker": ["AAPL"] * len(dates),
        "open": closes,
        "high": [value + 1 for value in closes],
        "low": [value - 1 for value in closes],
        "close": closes,
        "volume": [1_000_000] * len(dates),
        "bar_revision_id": bar_ids,
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=pd.Index([value.date() for value in dates], name="session_date"))
    pattern = {
        "type": "ASCENDING_TRIANGLE",
        "name": "Ascending triangle",
        "status": "FORMING",
        "bias": "BULLISH",
        "grade": "VALID_GEOMETRY",
        "start_time": int(pd.Timestamp(dates[5]).timestamp()),
        "end_time": int(pd.Timestamp(dates[-2]).timestamp()),
        "boundary_price": 100.0,
        "invalidation_price": 95.0,
        "readiness": "AT_EDGE",
    }
    signal_date = frame.index[-1]
    context = HistoricalSignalContext(
        signal_date=signal_date,
        signal_time=frame.iloc[-1]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v2",
        corporate_actions=(),
    )
    adapter = PatternBoundaryBreakDailyAdapter()
    adapter.minimum_bars = 31

    with patch(
        "research.historical_signal_replay.detect_forming_patterns",
        side_effect=([pattern], []),
    ) as detector:
        assert adapter.candidate_dates(frame) == (signal_date,)
        event = adapter.evaluate("AAPL", frame, context)[0]

    assert detector.call_count == 2
    assert event.source_name == "PATTERN_ASCENDING_TRIANGLE_BOUNDARY_BREAK"
    assert event.direction == 1
    assert event.source_bar_revision_ids == (bar_ids[-2], bar_ids[-1])
    assert event.payload["trigger_type"] == "BOUNDARY_BREAK"
    assert event.payload["boundary_price"] == 100.0
    assert event.payload["confirmation_close"] == 101.0
    assert event.payload["outcome_modes"] == ["DIRECTIONAL_HORIZON"]


def test_product_pullback_adapter_calls_page_scanner_with_fixed_daily_window() -> None:
    dates = pd.bdate_range("2023-01-03", periods=210)
    frame = pd.DataFrame({
        "open": [100.0] * 210, "high": [101.0] * 210,
        "low": [99.0] * 210, "close": [100.0] * 210,
        "volume": [1_000_000] * 210,
        "bar_revision_id": [uuid4() for _ in dates],
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=[value.date() for value in dates])
    adapter = MomentumPullbackAdapter()
    observed = {}

    def scanner(ticker, scanner_frame, *, interval):
        observed["ticker"] = ticker
        observed["index"] = scanner_frame.index
        observed["interval"] = interval
        return {"grade": "A", "score": 84.0, "last_close": 100.0}

    adapter.scanner = scanner
    signal_date = frame.index[-1]
    context = HistoricalSignalContext(
        signal_date=signal_date,
        signal_time=frame.iloc[-1]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v1",
        corporate_actions=(),
    )

    event = adapter.evaluate("AAPL", frame, context)[0]

    assert observed["ticker"] == "AAPL"
    assert observed["interval"] == "1d"
    assert observed["index"].equals(pd.DatetimeIndex(dates))
    assert event.direction == 1
    assert event.payload["grade"] == "A"
    assert event.payload["score"] == 84.0
    assert event.payload["indicator_window_bars"] == 210


def test_product_pullback_adapter_suppresses_contiguous_match() -> None:
    dates = pd.bdate_range("2023-01-03", periods=211)
    frame = pd.DataFrame({
        "open": [100.0] * 211, "high": [101.0] * 211,
        "low": [99.0] * 211, "close": [100.0] * 211,
        "volume": [1_000_000] * 211,
        "bar_revision_id": [uuid4() for _ in dates],
        "bar_end": [
            datetime.combine(value.date(), datetime.min.time(), tzinfo=UTC)
            for value in dates
        ],
    }, index=[value.date() for value in dates])
    adapter = MomentumPullbackAdapter()
    adapter.scanner = lambda *args, **kwargs: {
        "grade": "B", "score": 65.0, "last_close": 100.0,
    }
    signal_date = frame.index[-1]
    context = HistoricalSignalContext(
        signal_date=signal_date,
        signal_time=frame.iloc[-1]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v1",
        corporate_actions=(),
    )

    prior_date = frame.index[-2]
    prior_context = HistoricalSignalContext(
        signal_date=prior_date,
        signal_time=frame.iloc[-2]["bar_end"],
        universe_run_id=uuid4(),
        universe_policy_version="policy-v1",
        corporate_actions=(),
    )
    first = adapter.evaluate("AAPL", frame.iloc[:-1], prior_context)[0]
    second = adapter.evaluate("AAPL", frame, context)[0]

    from research.historical_signal_replay import _collapse_contiguous_events

    assert _collapse_contiguous_events((first, second)) == (first,)
    first_security = replace(first, payload=dict(first.payload, security_id=str(uuid4())))
    reused_ticker_security = replace(second, payload=dict(second.payload, security_id=str(uuid4())))
    assert _collapse_contiguous_events((first_security, reused_ticker_security)) == (first_security, reused_ticker_security)